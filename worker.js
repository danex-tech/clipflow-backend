require('dotenv').config();

const { Worker, UnrecoverableError } = require('bullmq');
const path = require('path');
const fs = require('fs');

const connection = require('./utils/redisConnection');

const {
  downloadCombined,
  downloadSingleFormat,
  downloadBestAudio,
} = require('./utils/ytdlp');

const {
  trimStream,
  mergeStreams,
  ensureAacAudio,
} = require('./utils/ffmpeg');

const DOWNLOADS_DIR = path.join(__dirname, 'downloads');

if (!fs.existsSync(DOWNLOADS_DIR)) {
  fs.mkdirSync(DOWNLOADS_DIR, { recursive: true });
}

const MAX_ATTEMPTS = 5;

// Platforms with a documented history of audio problems in downloaded
// files. For these, we spend a little extra time re-encoding the
// audio track to guarantee it's valid AAC.
function needsAudioSafety(url) {
  return /youtube\.com|youtu\.be|tiktok\.com/i.test(url);
}

function makeStageReporter(
  job,
  { rangeStart, rangeEnd, stage, attempt, maxAttempts }
) {
  let lastSent = -1;

  return (subPercent) => {
    const overall = Math.round(
      rangeStart + (subPercent / 100) * (rangeEnd - rangeStart)
    );

    if (overall !== lastSent) {
      lastSent = overall;

      job
        .updateProgress({
          stage,
          percent: overall,
          attempt,
          maxAttempts,
        })
        .catch(() => {});
    }
  };
}

// Deletes a file if it exists.
// Errors are intentionally ignored because the file may already be gone.
function safeUnlink(filePath) {
  if (!filePath) return;

  fs.unlink(filePath, () => {});
}

const worker = new Worker(
  'video-downloads',

  async (job) => {
    const {
      url,
      formatId,
      hasAudio,
      startTime,
      endTime,
      fileId,
      title,
      duration,
    } = job.data;

    const wantsTrim = Boolean(startTime || endTime);

    const audioCodec = needsAudioSafety(url) ? 'aac' : 'copy';

    const attempt = job.attemptsMade + 1;

    /*
     * IMPORTANT DOWNLOAD/RETRY DESIGN
     *
     * The download files MUST remain the same across retries.
     *
     * Example:
     *
     * Attempt 1:
     *   abc123-video.mp4 -> 70%
     *
     * Attempt 2:
     *   abc123-video.mp4 -> resumes from ~70%
     *
     * We intentionally do NOT put the attempt number in these
     * download filenames.
     *
     * fileId itself is already unique per job, so different jobs
     * cannot collide with each other.
     *
     * Processing/output files can still use the attempt number.
     */

    const runId = `${fileId}-a${attempt}`;

    // ------------------------------------------------------------
    // STABLE DOWNLOAD FILES
    // ------------------------------------------------------------
    //
    // These paths stay EXACTLY the same across retries.
    //
    const rawPath = path.join(
      DOWNLOADS_DIR,
      `${fileId}-raw.mp4`
    );

    const videoOnlyPath = path.join(
      DOWNLOADS_DIR,
      `${fileId}-video.mp4`
    );

    const audioOnlyPath = path.join(
      DOWNLOADS_DIR,
      `${fileId}-audio.m4a`
    );

    // ------------------------------------------------------------
    // ATTEMPT-SPECIFIC PROCESSING FILES
    // ------------------------------------------------------------
    //
    // These are safe to make unique per attempt because they are
    // created only after downloading and are not used for resume.
    //
    const trimmedVideoPath = path.join(
      DOWNLOADS_DIR,
      `${runId}-video-trimmed.mp4`
    );

    const trimmedAudioPath = path.join(
      DOWNLOADS_DIR,
      `${runId}-audio-trimmed.m4a`
    );

    // Final output is unique per JOB, not per attempt.
    //
    // This means retrying a job doesn't create:
    //   file-a1-final.mp4
    //   file-a2-final.mp4
    //   file-a3-final.mp4
    //
    // Instead, all retries ultimately produce:
    //   file-final.mp4
    //
    const finalPath = path.join(
      DOWNLOADS_DIR,
      `${fileId}-final.mp4`
    );

    // ------------------------------------------------------------
    // CANCELLATION SETUP
    // ------------------------------------------------------------

    await connection
      .del(`cancel:${job.id}`)
      .catch(() => {});

    let cancelled = false;

    const currentProcess = {
      current: null,
    };

    const cancelCheckInterval = setInterval(async () => {
      try {
        const flag = await connection.get(`cancel:${job.id}`);

        if (flag) {
          cancelled = true;

          if (currentProcess.current) {
            currentProcess.current.kill('SIGKILL');
          }
        }
      } catch {
        // Ignore transient Redis errors.
      }
    }, 2000);

    function throwIfCancelled() {
      if (cancelled) {
        throw new UnrecoverableError(
          'Job was cancelled by user'
        );
      }
    }

    /*
     * Files that belong ONLY to the current processing attempt.
     *
     * IMPORTANT:
     *
     * We DO NOT put the stable raw download files here.
     *
     * Otherwise the catch block would delete them and destroy
     * yt-dlp's ability to resume after a network failure.
     */
    const attemptTempFiles = [];

    try {
      // ----------------------------------------------------------
      // RETRY STATUS
      // ----------------------------------------------------------

      if (attempt > 1) {
        await job.updateProgress({
          stage: 'reconnecting',
          percent: 0,
          attempt,
          maxAttempts: MAX_ATTEMPTS,
          weakConnection: true,
        });
      }

      // ==========================================================
      // CASE 1: NO TRIMMING
      // ==========================================================

      if (!wantsTrim) {
        /*
         * Only finalPath is attempt-cleanup territory here.
         *
         * rawPath is intentionally NOT added.
         */
        attemptTempFiles.push(finalPath);

        const reportDownload = makeStageReporter(job, {
          rangeStart: 10,
          rangeEnd: 70,
          stage: 'downloading',
          attempt,
          maxAttempts: MAX_ATTEMPTS,
        });

        /*
         * On retry, don't pretend the actual download is starting
         * from zero.
         *
         * The UI can still use the retry/reconnecting state before
         * this point.
         */
        await job.updateProgress({
          stage: 'downloading',
          percent: attempt > 1 ? 10 : 10,
          attempt,
          maxAttempts: MAX_ATTEMPTS,
        });

        await downloadCombined({
          url,
          formatId,
          hasAudio,
          outputPath: rawPath,
          onProgress: reportDownload,
          processRef: currentProcess,
        });

        throwIfCancelled();

        // --------------------------------------------------------
        // AUDIO SAFETY FINALIZATION
        // --------------------------------------------------------

        if (needsAudioSafety(url)) {
          const reportFinal = makeStageReporter(job, {
            rangeStart: 70,
            rangeEnd: 99,
            stage: 'finalizing',
            attempt,
            maxAttempts: MAX_ATTEMPTS,
          });

          await ensureAacAudio({
            inputPath: rawPath,
            outputPath: finalPath,
            totalDurationSec: duration,
            onProgress: reportFinal,
            processRef: currentProcess,
          });

          throwIfCancelled();

          // Download is completely finished and final file exists.
          // Now it is safe to remove the raw source.
          safeUnlink(rawPath);
        } else {
          /*
           * No re-encoding needed.
           *
           * Rename the completed stable raw file into the final
           * unique output path.
           */
          fs.renameSync(rawPath, finalPath);
        }
      }

      // ==========================================================
      // CASE 2: TRIMMING + VIDEO ALREADY HAS AUDIO
      // ==========================================================

      else if (hasAudio) {
        /*
         * rawPath is stable across retries.
         *
         * finalPath is only cleaned if this attempt fails.
         */
        attemptTempFiles.push(finalPath);

        const reportDownload = makeStageReporter(job, {
          rangeStart: 10,
          rangeEnd: 60,
          stage: 'downloading',
          attempt,
          maxAttempts: MAX_ATTEMPTS,
        });

        await job.updateProgress({
          stage: 'downloading',
          percent: 10,
          attempt,
          maxAttempts: MAX_ATTEMPTS,
        });

        await downloadSingleFormat({
          url,
          formatId,
          outputPath: rawPath,
          onProgress: reportDownload,
          processRef: currentProcess,
        });

        throwIfCancelled();

        // --------------------------------------------------------
        // TRIM
        // --------------------------------------------------------

        const reportTrim = makeStageReporter(job, {
          rangeStart: 60,
          rangeEnd: 99,
          stage: 'trimming',
          attempt,
          maxAttempts: MAX_ATTEMPTS,
        });

        await trimStream({
          inputPath: rawPath,
          outputPath: finalPath,
          startTime,
          endTime,
          videoCodec: 'copy',
          audioCodec,
          onProgress: reportTrim,
          processRef: currentProcess,
        });

        throwIfCancelled();

        // Only remove the stable raw file AFTER successful trimming.
        safeUnlink(rawPath);
      }

      // ==========================================================
      // CASE 3: VIDEO + AUDIO ARE SEPARATE
      // ==========================================================

      else {
        /*
         * These downloaded files are also stable across retries.
         *
         * Example:
         *
         * attempt 1:
         *   abc-video.mp4  -> 60%
         *   connection dies
         *
         * attempt 2:
         *   abc-video.mp4  -> resumes
         *
         * The same applies to abc-audio.m4a.
         */

        attemptTempFiles.push(
          trimmedVideoPath,
          trimmedAudioPath,
          finalPath
        );

        // --------------------------------------------------------
        // VIDEO DOWNLOAD
        // --------------------------------------------------------

        const reportVideoDl = makeStageReporter(job, {
          rangeStart: 10,
          rangeEnd: 35,
          stage: 'downloading video',
          attempt,
          maxAttempts: MAX_ATTEMPTS,
        });

        await job.updateProgress({
          stage: 'downloading video',
          percent: 10,
          attempt,
          maxAttempts: MAX_ATTEMPTS,
        });

        await downloadSingleFormat({
          url,
          formatId,
          outputPath: videoOnlyPath,
          onProgress: reportVideoDl,
          processRef: currentProcess,
        });

        throwIfCancelled();

        // --------------------------------------------------------
        // AUDIO DOWNLOAD
        // --------------------------------------------------------

        const reportAudioDl = makeStageReporter(job, {
          rangeStart: 35,
          rangeEnd: 50,
          stage: 'downloading audio',
          attempt,
          maxAttempts: MAX_ATTEMPTS,
        });

        await job.updateProgress({
          stage: 'downloading audio',
          percent: 35,
          attempt,
          maxAttempts: MAX_ATTEMPTS,
        });

        await downloadBestAudio({
          url,
          outputPath: audioOnlyPath,
          onProgress: reportAudioDl,
          processRef: currentProcess,
        });

        throwIfCancelled();

        // --------------------------------------------------------
        // TRIM VIDEO
        // --------------------------------------------------------

        const reportVideoTrim = makeStageReporter(job, {
          rangeStart: 50,
          rangeEnd: 65,
          stage: 'trimming video',
          attempt,
          maxAttempts: MAX_ATTEMPTS,
        });

        await trimStream({
          inputPath: videoOnlyPath,
          outputPath: trimmedVideoPath,
          startTime,
          endTime,
          videoCodec: 'copy',
          audioCodec: 'copy',
          onProgress: reportVideoTrim,
          processRef: currentProcess,
        });

        throwIfCancelled();

        // Video is fully processed.
        safeUnlink(videoOnlyPath);

        // --------------------------------------------------------
        // TRIM AUDIO
        // --------------------------------------------------------

        const reportAudioTrim = makeStageReporter(job, {
          rangeStart: 65,
          rangeEnd: 80,
          stage: 'trimming audio',
          attempt,
          maxAttempts: MAX_ATTEMPTS,
        });

        await trimStream({
          inputPath: audioOnlyPath,
          outputPath: trimmedAudioPath,
          startTime,
          endTime,
          videoCodec: 'copy',
          audioCodec,
          onProgress: reportAudioTrim,
          processRef: currentProcess,
        });

        throwIfCancelled();

        // Audio is fully processed.
        safeUnlink(audioOnlyPath);

        // --------------------------------------------------------
        // MERGE
        // --------------------------------------------------------

        await job.updateProgress({
          stage: 'merging',
          percent: 90,
          attempt,
          maxAttempts: MAX_ATTEMPTS,
        });

        await mergeStreams({
          videoPath: trimmedVideoPath,
          audioPath: trimmedAudioPath,
          outputPath: finalPath,
          processRef: currentProcess,
        });

        throwIfCancelled();

        safeUnlink(trimmedVideoPath);
        safeUnlink(trimmedAudioPath);
      }

      // ==========================================================
      // SUCCESS
      // ==========================================================

      await job.updateProgress({
        stage: 'done',
        percent: 100,
        attempt,
        maxAttempts: MAX_ATTEMPTS,
      });

      /*
       * Success.
       *
       * Do NOT clean finalPath.
       * The API needs this file when /download/result/:jobId
       * is requested.
       */

      return {
        filePath: finalPath,
        title,
      };
    } catch (err) {
      /*
       * IMPORTANT RETRY BEHAVIOR
       *
       * We only delete files created by the CURRENT processing
       * attempt.
       *
       * We deliberately DO NOT delete:
       *
       *   rawPath
       *   videoOnlyPath
       *   audioOnlyPath
       *
       * because they are the stable download files that yt-dlp
       * needs to resume on the next BullMQ attempt.
       */

      for (const filePath of attemptTempFiles) {
        safeUnlink(filePath);
      }

      // ----------------------------------------------------------
      // CANCELLATION
      // ----------------------------------------------------------

      if (cancelled || err instanceof UnrecoverableError) {
        /*
         * Cancellation is permanent for this job, so now it is
         * safe to clean up the stable download files as well.
         */

        safeUnlink(rawPath);
        safeUnlink(videoOnlyPath);
        safeUnlink(audioOnlyPath);

        throw new UnrecoverableError(
          'Job was cancelled by user'
        );
      }

      // ----------------------------------------------------------
      // PROCESS KILLED
      // ----------------------------------------------------------

      if (err.message === 'PROCESS_KILLED') {
        /*
         * This is still a retryable failure.
         *
         * Do NOT delete the stable download files.
         */
        throw new Error(
          'A processing step was interrupted unexpectedly'
        );
      }

      /*
       * Normal error.
       *
       * BullMQ will retry this job because the queue was configured
       * with attempts: MAX_ATTEMPTS.
       *
       * Stable download files remain on disk.
       */
      throw err;
    } finally {
      clearInterval(cancelCheckInterval);
    }
  },

  {
    connection,

    // Give yt-dlp/ffmpeg plenty of time before BullMQ considers
    // the worker lock stale.
    lockDuration: 10 * 60 * 1000,

    // Render free tier is CPU/RAM constrained, so keep this at 1.
    concurrency: 1,
  }
);

// ================================================================
// WORKER EVENTS
// ================================================================

worker.on('completed', (job) => {
  console.log(
    `Job ${job.id} completed (attempts: ${job.attemptsMade + 1})`
  );
});

worker.on('failed', (job, err) => {
  console.error(
    `Job ${job?.id} failed after ${
      job ? job.attemptsMade + 1 : '?'
    } attempt(s):`,
    err.message
  );
});

worker.on('error', (err) => {
  console.error(
    '[Worker] internal error:',
    err.message
  );
});

worker.on('stalled', (jobId) => {
  console.warn(
    `[Worker] job ${jobId} stalled`
  );
});

worker.on('active', (job) => {
  console.log(
    `[Worker] picked up job ${job.id}`
  );
});

console.log(
  'Worker started, waiting for jobs...'
);  job,
  { rangeStart, rangeEnd, stage, attempt, maxAttempts }
) {
  let lastSent = -1;

  return (subPercent) => {
    const overall = Math.round(
      rangeStart + (subPercent / 100) * (rangeEnd - rangeStart)
    );

    if (overall !== lastSent) {
      lastSent = overall;

      job
        .updateProgress({
          stage,
          percent: overall,
          attempt,
          maxAttempts,
        })
        .catch(() => {});
    }
  };
}

// Deletes a file if it exists.
// Errors are intentionally ignored because the file may already be gone.
function safeUnlink(filePath) {
  if (!filePath) return;

  fs.unlink(filePath, () => {});
}

const worker = new Worker(
  'video-downloads',

  async (job) => {
    const {
      url,
      formatId,
      hasAudio,
      startTime,
      endTime,
      fileId,
      title,
      duration,
    } = job.data;

    const wantsTrim = Boolean(startTime || endTime);

    const audioCodec = needsAudioSafety(url) ? 'aac' : 'copy';

    const attempt = job.attemptsMade + 1;

    /*
     * IMPORTANT DOWNLOAD/RETRY DESIGN
     *
     * The download files MUST remain the same across retries.
     *
     * Example:
     *
     * Attempt 1:
     *   abc123-video.mp4 -> 70%
     *
     * Attempt 2:
     *   abc123-video.mp4 -> resumes from ~70%
     *
     * We intentionally do NOT put the attempt number in these
     * download filenames.
     *
     * fileId itself is already unique per job, so different jobs
     * cannot collide with each other.
     *
     * Processing/output files can still use the attempt number.
     */

    const runId = `${fileId}-a${attempt}`;

    // ------------------------------------------------------------
    // STABLE DOWNLOAD FILES
    // ------------------------------------------------------------
    //
    // These paths stay EXACTLY the same across retries.
    //
    const rawPath = path.join(
      DOWNLOADS_DIR,
      `${fileId}-raw.mp4`
    );

    const videoOnlyPath = path.join(
      DOWNLOADS_DIR,
      `${fileId}-video.mp4`
    );

    const audioOnlyPath = path.join(
      DOWNLOADS_DIR,
      `${fileId}-audio.m4a`
    );

    // ------------------------------------------------------------
    // ATTEMPT-SPECIFIC PROCESSING FILES
    // ------------------------------------------------------------
    //
    // These are safe to make unique per attempt because they are
    // created only after downloading and are not used for resume.
    //
    const trimmedVideoPath = path.join(
      DOWNLOADS_DIR,
      `${runId}-video-trimmed.mp4`
    );

    const trimmedAudioPath = path.join(
      DOWNLOADS_DIR,
      `${runId}-audio-trimmed.m4a`
    );

    // Final output is unique per JOB, not per attempt.
    //
    // This means retrying a job doesn't create:
    //   file-a1-final.mp4
    //   file-a2-final.mp4
    //   file-a3-final.mp4
    //
    // Instead, all retries ultimately produce:
    //   file-final.mp4
    //
    const finalPath = path.join(
      DOWNLOADS_DIR,
      `${fileId}-final.mp4`
    );

    // ------------------------------------------------------------
    // CANCELLATION SETUP
    // ------------------------------------------------------------

    await connection
      .del(`cancel:${job.id}`)
      .catch(() => {});

    let cancelled = false;

    const currentProcess = {
      current: null,
    };

    const cancelCheckInterval = setInterval(async () => {
      try {
        const flag = await connection.get(`cancel:${job.id}`);

        if (flag) {
          cancelled = true;

          if (currentProcess.current) {
            currentProcess.current.kill('SIGKILL');
          }
        }
      } catch {
        // Ignore transient Redis errors.
      }
    }, 2000);

    function throwIfCancelled() {
      if (cancelled) {
        throw new UnrecoverableError(
          'Job was cancelled by user'
        );
      }
    }

    /*
     * Files that belong ONLY to the current processing attempt.
     *
     * IMPORTANT:
     *
     * We DO NOT put the stable raw download files here.
     *
     * Otherwise the catch block would delete them and destroy
     * yt-dlp's ability to resume after a network failure.
     */
    const attemptTempFiles = [];

    try {
      // ----------------------------------------------------------
      // RETRY STATUS
      // ----------------------------------------------------------

      if (attempt > 1) {
        await job.updateProgress({
          stage: 'reconnecting',
          percent: 0,
          attempt,
          maxAttempts: MAX_ATTEMPTS,
          weakConnection: true,
        });
      }

      // ==========================================================
      // CASE 1: NO TRIMMING
      // ==========================================================

      if (!wantsTrim) {
        /*
         * Only finalPath is attempt-cleanup territory here.
         *
         * rawPath is intentionally NOT added.
         */
        attemptTempFiles.push(finalPath);

        const reportDownload = makeStageReporter(job, {
          rangeStart: 10,
          rangeEnd: 70,
          stage: 'downloading',
          attempt,
          maxAttempts: MAX_ATTEMPTS,
        });

        /*
         * On retry, don't pretend the actual download is starting
         * from zero.
         *
         * The UI can still use the retry/reconnecting state before
         * this point.
         */
        await job.updateProgress({
          stage: 'downloading',
          percent: attempt > 1 ? 10 : 10,
          attempt,
          maxAttempts: MAX_ATTEMPTS,
        });

        await downloadCombined({
          url,
          formatId,
          hasAudio,
          outputPath: rawPath,
          onProgress: reportDownload,
          processRef: currentProcess,
        });

        throwIfCancelled();

        // --------------------------------------------------------
        // AUDIO SAFETY FINALIZATION
        // --------------------------------------------------------

        if (needsAudioSafety(url)) {
          const reportFinal = makeStageReporter(job, {
            rangeStart: 70,
            rangeEnd: 99,
            stage: 'finalizing',
            attempt,
            maxAttempts: MAX_ATTEMPTS,
          });

          await ensureAacAudio({
            inputPath: rawPath,
            outputPath: finalPath,
            totalDurationSec: duration,
            onProgress: reportFinal,
            processRef: currentProcess,
          });

          throwIfCancelled();

          // Download is completely finished and final file exists.
          // Now it is safe to remove the raw source.
          safeUnlink(rawPath);
        } else {
          /*
           * No re-encoding needed.
           *
           * Rename the completed stable raw file into the final
           * unique output path.
           */
          fs.renameSync(rawPath, finalPath);
        }
      }

      // ==========================================================
      // CASE 2: TRIMMING + VIDEO ALREADY HAS AUDIO
      // ==========================================================

      else if (hasAudio) {
        /*
         * rawPath is stable across retries.
         *
         * finalPath is only cleaned if this attempt fails.
         */
        attemptTempFiles.push(finalPath);

        const reportDownload = makeStageReporter(job, {
          rangeStart: 10,
          rangeEnd: 60,
          stage: 'downloading',
          attempt,
          maxAttempts: MAX_ATTEMPTS,
        });

        await job.updateProgress({
          stage: 'downloading',
          percent: 10,
          attempt,
          maxAttempts: MAX_ATTEMPTS,
        });

        await downloadSingleFormat({
          url,
          formatId,
          outputPath: rawPath,
          onProgress: reportDownload,
          processRef: currentProcess,
        });

        throwIfCancelled();

        // --------------------------------------------------------
        // TRIM
        // --------------------------------------------------------

        const reportTrim = makeStageReporter(job, {
          rangeStart: 60,
          rangeEnd: 99,
          stage: 'trimming',
          attempt,
          maxAttempts: MAX_ATTEMPTS,
        });

        await trimStream({
          inputPath: rawPath,
          outputPath: finalPath,
          startTime,
          endTime,
          videoCodec: 'copy',
          audioCodec,
          onProgress: reportTrim,
          processRef: currentProcess,
        });

        throwIfCancelled();

        // Only remove the stable raw file AFTER successful trimming.
        safeUnlink(rawPath);
      }

      // ==========================================================
      // CASE 3: VIDEO + AUDIO ARE SEPARATE
      // ==========================================================

      else {
        /*
         * These downloaded files are also stable across retries.
         *
         * Example:
         *
         * attempt 1:
         *   abc-video.mp4  -> 60%
         *   connection dies
         *
         * attempt 2:
         *   abc-video.mp4  -> resumes
         *
         * The same applies to abc-audio.m4a.
         */

        attemptTempFiles.push(
          trimmedVideoPath,
          trimmedAudioPath,
          finalPath
        );

        // --------------------------------------------------------
        // VIDEO DOWNLOAD
        // --------------------------------------------------------

        const reportVideoDl = makeStageReporter(job, {
          rangeStart: 10,
          rangeEnd: 35,
          stage: 'downloading video',
          attempt,
          maxAttempts: MAX_ATTEMPTS,
        });

        await job.updateProgress({
          stage: 'downloading video',
          percent: 10,
          attempt,
          maxAttempts: MAX_ATTEMPTS,
        });

        await downloadSingleFormat({
          url,
          formatId,
          outputPath: videoOnlyPath,
          onProgress: reportVideoDl,
          processRef: currentProcess,
        });

        throwIfCancelled();

        // --------------------------------------------------------
        // AUDIO DOWNLOAD
        // --------------------------------------------------------

        const reportAudioDl = makeStageReporter(job, {
          rangeStart: 35,
          rangeEnd: 50,
          stage: 'downloading audio',
          attempt,
          maxAttempts: MAX_ATTEMPTS,
        });

        await job.updateProgress({
          stage: 'downloading audio',
          percent: 35,
          attempt,
          maxAttempts: MAX_ATTEMPTS,
        });

        await downloadBestAudio({
          url,
          outputPath: audioOnlyPath,
          onProgress: reportAudioDl,
          processRef: currentProcess,
        });

        throwIfCancelled();

        // --------------------------------------------------------
        // TRIM VIDEO
        // --------------------------------------------------------

        const reportVideoTrim = makeStageReporter(job, {
          rangeStart: 50,
          rangeEnd: 65,
          stage: 'trimming video',
          attempt,
          maxAttempts: MAX_ATTEMPTS,
        });

        await trimStream({
          inputPath: videoOnlyPath,
          outputPath: trimmedVideoPath,
          startTime,
          endTime,
          videoCodec: 'copy',
          audioCodec: 'copy',
          onProgress: reportVideoTrim,
          processRef: currentProcess,
        });

        throwIfCancelled();

        // Video is fully processed.
        safeUnlink(videoOnlyPath);

        // --------------------------------------------------------
        // TRIM AUDIO
        // --------------------------------------------------------

        const reportAudioTrim = makeStageReporter(job, {
          rangeStart: 65,
          rangeEnd: 80,
          stage: 'trimming audio',
          attempt,
          maxAttempts: MAX_ATTEMPTS,
        });

        await trimStream({
          inputPath: audioOnlyPath,
          outputPath: trimmedAudioPath,
          startTime,
          endTime,
          videoCodec: 'copy',
          audioCodec,
          onProgress: reportAudioTrim,
          processRef: currentProcess,
        });

        throwIfCancelled();

        // Audio is fully processed.
        safeUnlink(audioOnlyPath);

        // --------------------------------------------------------
        // MERGE
        // --------------------------------------------------------

        await job.updateProgress({
          stage: 'merging',
          percent: 90,
          attempt,
          maxAttempts: MAX_ATTEMPTS,
        });

        await mergeStreams({
          videoPath: trimmedVideoPath,
          audioPath: trimmedAudioPath,
          outputPath: finalPath,
          processRef: currentProcess,
        });

        throwIfCancelled();

        safeUnlink(trimmedVideoPath);
        safeUnlink(trimmedAudioPath);
      }

      // ==========================================================
      // SUCCESS
      // ==========================================================

      await job.updateProgress({
        stage: 'done',
        percent: 100,
        attempt,
        maxAttempts: MAX_ATTEMPTS,
      });

      /*
       * Success.
       *
       * Do NOT clean finalPath.
       * The API needs this file when /download/result/:jobId
       * is requested.
       */

      return {
        filePath: finalPath,
        title,
      };
    } catch (err) {
      /*
       * IMPORTANT RETRY BEHAVIOR
       *
       * We only delete files created by the CURRENT processing
       * attempt.
       *
       * We deliberately DO NOT delete:
       *
       *   rawPath
       *   videoOnlyPath
       *   audioOnlyPath
       *
       * because they are the stable download files that yt-dlp
       * needs to resume on the next BullMQ attempt.
       */

      for (const filePath of attemptTempFiles) {
        safeUnlink(filePath);
      }

      // ----------------------------------------------------------
      // CANCELLATION
      // ----------------------------------------------------------

      if (cancelled || err instanceof UnrecoverableError) {
        /*
         * Cancellation is permanent for this job, so now it is
         * safe to clean up the stable download files as well.
         */

        safeUnlink(rawPath);
        safeUnlink(videoOnlyPath);
        safeUnlink(audioOnlyPath);

        throw new UnrecoverableError(
          'Job was cancelled by user'
        );
      }

      // ----------------------------------------------------------
      // PROCESS KILLED
      // ----------------------------------------------------------

      if (err.message === 'PROCESS_KILLED') {
        /*
         * This is still a retryable failure.
         *
         * Do NOT delete the stable download files.
         */
        throw new Error(
          'A processing step was interrupted unexpectedly'
        );
      }

      /*
       * Normal error.
       *
       * BullMQ will retry this job because the queue was configured
       * with attempts: MAX_ATTEMPTS.
       *
       * Stable download files remain on disk.
       */
      throw err;
    } finally {
      clearInterval(cancelCheckInterval);
    }
  },

  {
    connection,

    // Give yt-dlp/ffmpeg plenty of time before BullMQ considers
    // the worker lock stale.
    lockDuration: 10 * 60 * 1000,

    // Render free tier is CPU/RAM constrained, so keep this at 1.
    concurrency: 1,
  }
);

// ================================================================
// WORKER EVENTS
// ================================================================

worker.on('completed', (job) => {
  console.log(
    `Job ${job.id} completed (attempts: ${job.attemptsMade + 1})`
  );
});

worker.on('failed', (job, err) => {
  console.error(
    `Job ${job?.id} failed after ${
      job ? job.attemptsMade + 1 : '?'
    } attempt(s):`,
    err.message
  );
});

worker.on('error', (err) => {
  console.error(
    '[Worker] internal error:',
    err.message
  );
});

worker.on('stalled', (jobId) => {
  console.warn(
    `[Worker] job ${jobId} stalled`
  );
});

worker.on('active', (job) => {
  console.log(
    `[Worker] picked up job ${job.id}`
  );
});

console.log(
  'Worker started, waiting for jobs...'
);
  job,
  { rangeStart, rangeEnd, stage, attempt, maxAttempts }
) {
  let lastSent = -1;

  return (subPercent) => {
    const overall = Math.round(
      rangeStart + (subPercent / 100) * (rangeEnd - rangeStart)
    );

    if (overall !== lastSent) {
      lastSent = overall;

      job
        .updateProgress({
          stage,
          percent: overall,
          attempt,
          maxAttempts,
        })
        .catch(() => {});
    }
  };
}

// Deletes a file if it exists.
// Errors are intentionally ignored because the file may already be gone.
function safeUnlink(filePath) {
  if (!filePath) return;

  fs.unlink(filePath, () => {});
}

const worker = new Worker(
  'video-downloads',

  async (job) => {
    const {
      url,
      formatId,
      hasAudio,
      startTime,
      endTime,
      fileId,
      title,
      duration,
    } = job.data;

    const wantsTrim = Boolean(startTime || endTime);

    const audioCodec = needsAudioSafety(url) ? 'aac' : 'copy';

    const attempt = job.attemptsMade + 1;

    /*
     * IMPORTANT DOWNLOAD/RETRY DESIGN
     *
     * The download files MUST remain the same across retries.
     *
     * Example:
     *
     * Attempt 1:
     *   abc123-video.mp4 -> 70%
     *
     * Attempt 2:
     *   abc123-video.mp4 -> resumes from ~70%
     *
     * We intentionally do NOT put the attempt number in these
     * download filenames.
     *
     * fileId itself is already unique per job, so different jobs
     * cannot collide with each other.
     *
     * Processing/output files can still use the attempt number.
     */

    const runId = `${fileId}-a${attempt}`;

    // ------------------------------------------------------------
    // STABLE DOWNLOAD FILES
    // ------------------------------------------------------------
    //
    // These paths stay EXACTLY the same across retries.
    //
    const rawPath = path.join(
      DOWNLOADS_DIR,
      `${fileId}-raw.mp4`
    );

    const videoOnlyPath = path.join(
      DOWNLOADS_DIR,
      `${fileId}-video.mp4`
    );

    const audioOnlyPath = path.join(
      DOWNLOADS_DIR,
      `${fileId}-audio.m4a`
    );

    // ------------------------------------------------------------
    // ATTEMPT-SPECIFIC PROCESSING FILES
    // ------------------------------------------------------------
    //
    // These are safe to make unique per attempt because they are
    // created only after downloading and are not used for resume.
    //
    const trimmedVideoPath = path.join(
      DOWNLOADS_DIR,
      `${runId}-video-trimmed.mp4`
    );

    const trimmedAudioPath = path.join(
      DOWNLOADS_DIR,
      `${runId}-audio-trimmed.m4a`
    );

    // Final output is unique per JOB, not per attempt.
    //
    // This means retrying a job doesn't create:
    //   file-a1-final.mp4
    //   file-a2-final.mp4
    //   file-a3-final.mp4
    //
    // Instead, all retries ultimately produce:
    //   file-final.mp4
    //
    const finalPath = path.join(
      DOWNLOADS_DIR,
      `${fileId}-final.mp4`
    );

    // ------------------------------------------------------------
    // CANCELLATION SETUP
    // ------------------------------------------------------------

    await connection
      .del(`cancel:${job.id}`)
      .catch(() => {});

    let cancelled = false;

    const currentProcess = {
      current: null,
    };

    const cancelCheckInterval = setInterval(async () => {
      try {
        const flag = await connection.get(`cancel:${job.id}`);

        if (flag) {
          cancelled = true;

          if (currentProcess.current) {
            currentProcess.current.kill('SIGKILL');
          }
        }
      } catch {
        // Ignore transient Redis errors.
      }
    }, 2000);

    function throwIfCancelled() {
      if (cancelled) {
        throw new UnrecoverableError(
          'Job was cancelled by user'
        );
      }
    }

    /*
     * Files that belong ONLY to the current processing attempt.
     *
     * IMPORTANT:
     *
     * We DO NOT put the stable raw download files here.
     *
     * Otherwise the catch block would delete them and destroy
     * yt-dlp's ability to resume after a network failure.
     */
    const attemptTempFiles = [];

    try {
      // ----------------------------------------------------------
      // RETRY STATUS
      // ----------------------------------------------------------

      if (attempt > 1) {
        await job.updateProgress({
          stage: 'reconnecting',
          percent: 0,
          attempt,
          maxAttempts: MAX_ATTEMPTS,
          weakConnection: true,
        });
      }

      // ==========================================================
      // CASE 1: NO TRIMMING
      // ==========================================================

      if (!wantsTrim) {
        /*
         * Only finalPath is attempt-cleanup territory here.
         *
         * rawPath is intentionally NOT added.
         */
        attemptTempFiles.push(finalPath);

        const reportDownload = makeStageReporter(job, {
          rangeStart: 10,
          rangeEnd: 70,
          stage: 'downloading',
          attempt,
          maxAttempts: MAX_ATTEMPTS,
        });

        /*
         * On retry, don't pretend the actual download is starting
         * from zero.
         *
         * The UI can still use the retry/reconnecting state before
         * this point.
         */
        await job.updateProgress({
          stage: 'downloading',
          percent: attempt > 1 ? 10 : 10,
          attempt,
          maxAttempts: MAX_ATTEMPTS,
        });

        await downloadCombined({
          url,
          formatId,
          hasAudio,
          outputPath: rawPath,
          onProgress: reportDownload,
          processRef: currentProcess,
        });

        throwIfCancelled();

        // --------------------------------------------------------
        // AUDIO SAFETY FINALIZATION
        // --------------------------------------------------------

        if (needsAudioSafety(url)) {
          const reportFinal = makeStageReporter(job, {
            rangeStart: 70,
            rangeEnd: 99,
            stage: 'finalizing',
            attempt,
            maxAttempts: MAX_ATTEMPTS,
          });

          await ensureAacAudio({
            inputPath: rawPath,
            outputPath: finalPath,
            totalDurationSec: duration,
            onProgress: reportFinal,
            processRef: currentProcess,
          });

          throwIfCancelled();

          // Download is completely finished and final file exists.
          // Now it is safe to remove the raw source.
          safeUnlink(rawPath);
        } else {
          /*
           * No re-encoding needed.
           *
           * Rename the completed stable raw file into the final
           * unique output path.
           */
          fs.renameSync(rawPath, finalPath);
        }
      }

      // ==========================================================
      // CASE 2: TRIMMING + VIDEO ALREADY HAS AUDIO
      // ==========================================================

      else if (hasAudio) {
        /*
         * rawPath is stable across retries.
         *
         * finalPath is only cleaned if this attempt fails.
         */
        attemptTempFiles.push(finalPath);

        const reportDownload = makeStageReporter(job, {
          rangeStart: 10,
          rangeEnd: 60,
          stage: 'downloading',
          attempt,
          maxAttempts: MAX_ATTEMPTS,
        });

        await job.updateProgress({
          stage: 'downloading',
          percent: 10,
          attempt,
          maxAttempts: MAX_ATTEMPTS,
        });

        await downloadSingleFormat({
          url,
          formatId,
          outputPath: rawPath,
          onProgress: reportDownload,
          processRef: currentProcess,
        });

        throwIfCancelled();

        // --------------------------------------------------------
        // TRIM
        // --------------------------------------------------------

        const reportTrim = makeStageReporter(job, {
          rangeStart: 60,
          rangeEnd: 99,
          stage: 'trimming',
          attempt,
          maxAttempts: MAX_ATTEMPTS,
        });

        await trimStream({
          inputPath: rawPath,
          outputPath: finalPath,
          startTime,
          endTime,
          videoCodec: 'copy',
          audioCodec,
          onProgress: reportTrim,
          processRef: currentProcess,
        });

        throwIfCancelled();

        // Only remove the stable raw file AFTER successful trimming.
        safeUnlink(rawPath);
      }

      // ==========================================================
      // CASE 3: VIDEO + AUDIO ARE SEPARATE
      // ==========================================================

      else {
        /*
         * These downloaded files are also stable across retries.
         *
         * Example:
         *
         * attempt 1:
         *   abc-video.mp4  -> 60%
         *   connection dies
         *
         * attempt 2:
         *   abc-video.mp4  -> resumes
         *
         * The same applies to abc-audio.m4a.
         */

        attemptTempFiles.push(
          trimmedVideoPath,
          trimmedAudioPath,
          finalPath
        );

        // --------------------------------------------------------
        // VIDEO DOWNLOAD
        // --------------------------------------------------------

        const reportVideoDl = makeStageReporter(job, {
          rangeStart: 10,
          rangeEnd: 35,
          stage: 'downloading video',
          attempt,
          maxAttempts: MAX_ATTEMPTS,
        });

        await job.updateProgress({
          stage: 'downloading video',
          percent: 10,
          attempt,
          maxAttempts: MAX_ATTEMPTS,
        });

        await downloadSingleFormat({
          url,
          formatId,
          outputPath: videoOnlyPath,
          onProgress: reportVideoDl,
          processRef: currentProcess,
        });

        throwIfCancelled();

        // --------------------------------------------------------
        // AUDIO DOWNLOAD
        // --------------------------------------------------------

        const reportAudioDl = makeStageReporter(job, {
          rangeStart: 35,
          rangeEnd: 50,
          stage: 'downloading audio',
          attempt,
          maxAttempts: MAX_ATTEMPTS,
        });

        await job.updateProgress({
          stage: 'downloading audio',
          percent: 35,
          attempt,
          maxAttempts: MAX_ATTEMPTS,
        });

        await downloadBestAudio({
          url,
          outputPath: audioOnlyPath,
          onProgress: reportAudioDl,
          processRef: currentProcess,
        });

        throwIfCancelled();

        // --------------------------------------------------------
        // TRIM VIDEO
        // --------------------------------------------------------

        const reportVideoTrim = makeStageReporter(job, {
          rangeStart: 50,
          rangeEnd: 65,
          stage: 'trimming video',
          attempt,
          maxAttempts: MAX_ATTEMPTS,
        });

        await trimStream({
          inputPath: videoOnlyPath,
          outputPath: trimmedVideoPath,
          startTime,
          endTime,
          videoCodec: 'copy',
          audioCodec: 'copy',
          onProgress: reportVideoTrim,
          processRef: currentProcess,
        });

        throwIfCancelled();

        // Video is fully processed.
        safeUnlink(videoOnlyPath);

        // --------------------------------------------------------
        // TRIM AUDIO
        // --------------------------------------------------------

        const reportAudioTrim = makeStageReporter(job, {
          rangeStart: 65,
          rangeEnd: 80,
          stage: 'trimming audio',
          attempt,
          maxAttempts: MAX_ATTEMPTS,
        });

        await trimStream({
          inputPath: audioOnlyPath,
          outputPath: trimmedAudioPath,
          startTime,
          endTime,
          videoCodec: 'copy',
          audioCodec,
          onProgress: reportAudioTrim,
          processRef: currentProcess,
        });

        throwIfCancelled();

        // Audio is fully processed.
        safeUnlink(audioOnlyPath);

        // --------------------------------------------------------
        // MERGE
        // --------------------------------------------------------

        await job.updateProgress({
          stage: 'merging',
          percent: 90,
          attempt,
          maxAttempts: MAX_ATTEMPTS,
        });

        await mergeStreams({
          videoPath: trimmedVideoPath,
          audioPath: trimmedAudioPath,
          outputPath: finalPath,
          processRef: currentProcess,
        });

        throwIfCancelled();

        safeUnlink(trimmedVideoPath);
        safeUnlink(trimmedAudioPath);
      }

      // ==========================================================
      // SUCCESS
      // ==========================================================

      await job.updateProgress({
        stage: 'done',
        percent: 100,
        attempt,
        maxAttempts: MAX_ATTEMPTS,
      });

      /*
       * Success.
       *
       * Do NOT clean finalPath.
       * The API needs this file when /download/result/:jobId
       * is requested.
       */

      return {
        filePath: finalPath,
        title,
      };
    } catch (err) {
      /*
       * IMPORTANT RETRY BEHAVIOR
       *
       * We only delete files created by the CURRENT processing
       * attempt.
       *
       * We deliberately DO NOT delete:
       *
       *   rawPath
       *   videoOnlyPath
       *   audioOnlyPath
       *
       * because they are the stable download files that yt-dlp
       * needs to resume on the next BullMQ attempt.
       */

      for (const filePath of attemptTempFiles) {
        safeUnlink(filePath);
      }

      // ----------------------------------------------------------
      // CANCELLATION
      // ----------------------------------------------------------

      if (cancelled || err instanceof UnrecoverableError) {
        /*
         * Cancellation is permanent for this job, so now it is
         * safe to clean up the stable download files as well.
         */

        safeUnlink(rawPath);
        safeUnlink(videoOnlyPath);
        safeUnlink(audioOnlyPath);

        throw new UnrecoverableError(
          'Job was cancelled by user'
        );
      }

      // ----------------------------------------------------------
      // PROCESS KILLED
      // ----------------------------------------------------------

      if (err.message === 'PROCESS_KILLED') {
        /*
         * This is still a retryable failure.
         *
         * Do NOT delete the stable download files.
         */
        throw new Error(
          'A processing step was interrupted unexpectedly'
        );
      }

      /*
       * Normal error.
       *
       * BullMQ will retry this job because the queue was configured
       * with attempts: MAX_ATTEMPTS.
       *
       * Stable download files remain on disk.
       */
      throw err;
    } finally {
      clearInterval(cancelCheckInterval);
    }
  },

  {
    connection,

    // Give yt-dlp/ffmpeg plenty of time before BullMQ considers
    // the worker lock stale.
    lockDuration: 10 * 60 * 1000,

    // Render free tier is CPU/RAM constrained, so keep this at 1.
    concurrency: 1,
  }
);

// ================================================================
// WORKER EVENTS
// ================================================================

worker.on('completed', (job) => {
  console.log(
    `Job ${job.id} completed (attempts: ${job.attemptsMade + 1})`
  );
});

worker.on('failed', (job, err) => {
  console.error(
    `Job ${job?.id} failed after ${
      job ? job.attemptsMade + 1 : '?'
    } attempt(s):`,
    err.message
  );
});

worker.on('error', (err) => {
  console.error(
    '[Worker] internal error:',
    err.message
  );
});

worker.on('stalled', (jobId) => {
  console.warn(
    `[Worker] job ${jobId} stalled`
  );
});

worker.on('active', (job) => {
  console.log(
    `[Worker] picked up job ${job.id}`
  );
});

console.log(
  'Worker started, waiting for jobs...'
);

require('dotenv').config();

const {
  Worker,
  UnrecoverableError,
} = require('bullmq');

const path = require('path');
const fs = require('fs');

const connection =
  require('./utils/redisConnection');

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

// ================================================================
// CONFIG
// ================================================================

const DOWNLOADS_DIR =
  path.join(
    __dirname,
    'downloads'
  );

if (
  !fs.existsSync(
    DOWNLOADS_DIR
  )
) {
  fs.mkdirSync(
    DOWNLOADS_DIR,
    {
      recursive: true,
    }
  );
}

const MAX_ATTEMPTS = 5;

// ================================================================
// SERVER IDENTITY
// ================================================================
//
// Server A:
// SERVER_ID=A
// SERVER_PUBLIC_URL=https://clipflow-backend-nwrd.onrender.com/api
//
// Server B:
// SERVER_ID=B
// SERVER_PUBLIC_URL=https://clipflow-backend-2.onrender.com/api
//

const SERVER_ID =
  process.env.SERVER_ID ||
  'unknown';

const SERVER_PUBLIC_URL =
  process.env.SERVER_PUBLIC_URL ||
  '';

// ================================================================
// AUDIO SAFETY
// ================================================================

function needsAudioSafety(
  url
) {
  return /youtube\.com|youtu\.be|tiktok\.com/i.test(
    url
  );
}

// ================================================================
// PROGRESS REPORTER
// ================================================================

function makeStageReporter(
  job,
  {
    rangeStart,
    rangeEnd,
    stage,
    attempt,
    maxAttempts,
  }
) {
  let lastSent = -1;

  return (
    subPercent
  ) => {
    const overall =
      Math.round(
        rangeStart +
          (subPercent / 100) *
            (rangeEnd -
              rangeStart)
      );

    if (
      overall !== lastSent
    ) {
      lastSent =
        overall;

      job
        .updateProgress({
          stage,
          percent:
            overall,
          attempt,
          maxAttempts,
          serverId:
            SERVER_ID,
          serverUrl:
            SERVER_PUBLIC_URL,
        })
        .catch(() => {});
    }
  };
}

// ================================================================
// SAFE DELETE
// ================================================================

function safeUnlink(
  filePath
) {
  if (!filePath) {
    return;
  }

  fs.unlink(
    filePath,
    () => {}
  );
}

// ================================================================
// WORKER
// ================================================================

const worker =
  new Worker(
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

      const wantsTrim =
        Boolean(
          startTime ||
          endTime
        );

      const audioCodec =
        needsAudioSafety(url)
          ? 'aac'
          : 'copy';

      const attempt =
        job.attemptsMade +
        1;

      // ==========================================================
      // UNIQUE RUN ID
      // ==========================================================

      const runId =
        `${fileId}-a${attempt}`;

      // ==========================================================
      // STABLE DOWNLOAD FILES
      // ==========================================================
      //
      // These MUST stay the same across retries.
      //
      // This is what allows yt-dlp to resume.
      //

      const rawPath =
        path.join(
          DOWNLOADS_DIR,
          `${fileId}-raw.mp4`
        );

      const videoOnlyPath =
        path.join(
          DOWNLOADS_DIR,
          `${fileId}-video.mp4`
        );

      const audioOnlyPath =
        path.join(
          DOWNLOADS_DIR,
          `${fileId}-audio.m4a`
        );

      // ==========================================================
      // ATTEMPT-SPECIFIC PROCESSING FILES
      // ==========================================================

      const trimmedVideoPath =
        path.join(
          DOWNLOADS_DIR,
          `${runId}-video-trimmed.mp4`
        );

      const trimmedAudioPath =
        path.join(
          DOWNLOADS_DIR,
          `${runId}-audio-trimmed.m4a`
        );

      // ==========================================================
      // FINAL FILE
      // ==========================================================

      const finalPath =
        path.join(
          DOWNLOADS_DIR,
          `${fileId}-final.mp4`
        );

      // ==========================================================
      // CANCELLATION
      // ==========================================================

      await connection
        .del(
          `cancel:${job.id}`
        )
        .catch(() => {});

      let cancelled =
        false;

      const currentProcess = {
        current: null,
      };

      const cancelCheckInterval =
        setInterval(
          async () => {
            try {
              const flag =
                await connection.get(
                  `cancel:${job.id}`
                );

              if (flag) {
                cancelled =
                  true;

                if (
                  currentProcess.current
                ) {
                  currentProcess.current.kill(
                    'SIGKILL'
                  );
                }
              }
            } catch {
              // Ignore Redis errors.
            }
          },
          2000
        );

      function throwIfCancelled() {
        if (cancelled) {
          throw new UnrecoverableError(
            'Job was cancelled by user'
          );
        }
      }

      // ==========================================================
      // CURRENT ATTEMPT FILES
      // ==========================================================

      const attemptTempFiles =
        [];

      try {
        // ========================================================
        // RETRY STATUS
        // ========================================================

        if (
          attempt > 1
        ) {
          await job.updateProgress({
            stage:
              'reconnecting',
            percent: 0,
            attempt,
            maxAttempts:
              MAX_ATTEMPTS,
            weakConnection:
              true,
            serverId:
              SERVER_ID,
            serverUrl:
              SERVER_PUBLIC_URL,
          });
        }

        // ========================================================
        // CASE 1
        // NO TRIMMING
        // ========================================================

        if (
          !wantsTrim
        ) {
          attemptTempFiles.push(
            finalPath
          );

          const reportDownload =
            makeStageReporter(
              job,
              {
                rangeStart:
                  10,
                rangeEnd:
                  70,
                stage:
                  'downloading',
                attempt,
                maxAttempts:
                  MAX_ATTEMPTS,
              }
            );

          await job.updateProgress({
            stage:
              'downloading',
            percent: 10,
            attempt,
            maxAttempts:
              MAX_ATTEMPTS,
            serverId:
              SERVER_ID,
            serverUrl:
              SERVER_PUBLIC_URL,
          });

          await downloadCombined({
            url,
            formatId,
            hasAudio,
            outputPath:
              rawPath,
            onProgress:
              reportDownload,
            processRef:
              currentProcess,
          });

          throwIfCancelled();

          // ======================================================
          // AUDIO SAFETY
          // ======================================================

          if (
            needsAudioSafety(
              url
            )
          ) {
            const reportFinal =
              makeStageReporter(
                job,
                {
                  rangeStart:
                    70,
                  rangeEnd:
                    99,
                  stage:
                    'finalizing',
                  attempt,
                  maxAttempts:
                    MAX_ATTEMPTS,
                }
              );

            await ensureAacAudio({
              inputPath:
                rawPath,
              outputPath:
                finalPath,
              totalDurationSec:
                duration,
              onProgress:
                reportFinal,
              processRef:
                currentProcess,
            });

            throwIfCancelled();

            safeUnlink(
              rawPath
            );
          } else {
            fs.renameSync(
              rawPath,
              finalPath
            );
          }
        }

        // ========================================================
        // CASE 2
        // TRIMMING + VIDEO HAS AUDIO
        // ========================================================

        else if (
          hasAudio
        ) {
          attemptTempFiles.push(
            finalPath
          );

          const reportDownload =
            makeStageReporter(
              job,
              {
                rangeStart:
                  10,
                rangeEnd:
                  60,
                stage:
                  'downloading',
                attempt,
                maxAttempts:
                  MAX_ATTEMPTS,
              }
            );

          await job.updateProgress({
            stage:
              'downloading',
            percent: 10,
            attempt,
            maxAttempts:
              MAX_ATTEMPTS,
            serverId:
              SERVER_ID,
            serverUrl:
              SERVER_PUBLIC_URL,
          });

          await downloadSingleFormat({
            url,
            formatId,
            outputPath:
              rawPath,
            onProgress:
              reportDownload,
            processRef:
              currentProcess,
          });

          throwIfCancelled();

          // ======================================================
          // TRIM
          // ======================================================

          const reportTrim =
            makeStageReporter(
              job,
              {
                rangeStart:
                  60,
                rangeEnd:
                  99,
                stage:
                  'trimming',
                attempt,
                maxAttempts:
                  MAX_ATTEMPTS,
              }
            );

          await trimStream({
            inputPath:
              rawPath,
            outputPath:
              finalPath,
            startTime,
            endTime,
            videoCodec:
              'copy',
            audioCodec,
            onProgress:
              reportTrim,
            processRef:
              currentProcess,
          });

          throwIfCancelled();

          safeUnlink(
            rawPath
          );
        }

        // ========================================================
        // CASE 3
        // VIDEO + AUDIO SEPARATE
        // ========================================================

        else {
          attemptTempFiles.push(
            trimmedVideoPath,
            trimmedAudioPath,
            finalPath
          );

          // ======================================================
          // VIDEO DOWNLOAD
          // ======================================================

          const reportVideoDl =
            makeStageReporter(
              job,
              {
                rangeStart:
                  10,
                rangeEnd:
                  35,
                stage:
                  'downloading video',
                attempt,
                maxAttempts:
                  MAX_ATTEMPTS,
              }
            );

          await job.updateProgress({
            stage:
              'downloading video',
            percent: 10,
            attempt,
            maxAttempts:
              MAX_ATTEMPTS,
            serverId:
              SERVER_ID,
            serverUrl:
              SERVER_PUBLIC_URL,
          });

          await downloadSingleFormat({
            url,
            formatId,
            outputPath:
              videoOnlyPath,
            onProgress:
              reportVideoDl,
            processRef:
              currentProcess,
          });

          throwIfCancelled();

          // ======================================================
          // AUDIO DOWNLOAD
          // ======================================================

          const reportAudioDl =
            makeStageReporter(
              job,
              {
                rangeStart:
                  35,
                rangeEnd:
                  50,
                stage:
                  'downloading audio',
                attempt,
                maxAttempts:
                  MAX_ATTEMPTS,
              }
            );

          await job.updateProgress({
            stage:
              'downloading audio',
            percent: 35,
            attempt,
            maxAttempts:
              MAX_ATTEMPTS,
            serverId:
              SERVER_ID,
            serverUrl:
              SERVER_PUBLIC_URL,
          });

          await downloadBestAudio({
            url,
            outputPath:
              audioOnlyPath,
            onProgress:
              reportAudioDl,
            processRef:
              currentProcess,
          });

          throwIfCancelled();

          // ======================================================
          // TRIM VIDEO
          // ======================================================

          const reportVideoTrim =
            makeStageReporter(
              job,
              {
                rangeStart:
                  50,
                rangeEnd:
                  65,
                stage:
                  'trimming video',
                attempt,
                maxAttempts:
                  MAX_ATTEMPTS,
              }
            );

          await trimStream({
            inputPath:
              videoOnlyPath,
            outputPath:
              trimmedVideoPath,
            startTime,
            endTime,
            videoCodec:
              'copy',
            audioCodec:
              'copy',
            onProgress:
              reportVideoTrim,
            processRef:
              currentProcess,
          });

          throwIfCancelled();

          safeUnlink(
            videoOnlyPath
          );

          // ======================================================
          // TRIM AUDIO
          // ======================================================

          const reportAudioTrim =
            makeStageReporter(
              job,
              {
                rangeStart:
                  65,
                rangeEnd:
                  80,
                stage:
                  'trimming audio',
                attempt,
                maxAttempts:
                  MAX_ATTEMPTS,
              }
            );

          await trimStream({
            inputPath:
              audioOnlyPath,
            outputPath:
              trimmedAudioPath,
            startTime,
            endTime,
            videoCodec:
              'copy',
            audioCodec,
            onProgress:
              reportAudioTrim,
            processRef:
              currentProcess,
          });

          throwIfCancelled();

          safeUnlink(
            audioOnlyPath
          );

          // ======================================================
          // MERGE
          // ======================================================

          await job.updateProgress({
            stage:
              'merging',
            percent: 90,
            attempt,
            maxAttempts:
              MAX_ATTEMPTS,
            serverId:
              SERVER_ID,
            serverUrl:
              SERVER_PUBLIC_URL,
          });

          await mergeStreams({
            videoPath:
              trimmedVideoPath,
            audioPath:
              trimmedAudioPath,
            outputPath:
              finalPath,
            processRef:
              currentProcess,
          });

          throwIfCancelled();

          safeUnlink(
            trimmedVideoPath
          );

          safeUnlink(
            trimmedAudioPath
          );
        }

        // ========================================================
        // SUCCESS
        // ========================================================

        await job.updateProgress({
          stage:
            'done',
          percent: 100,
          attempt,
          maxAttempts:
            MAX_ATTEMPTS,
          serverId:
            SERVER_ID,
          serverUrl:
            SERVER_PUBLIC_URL,
        });

        // ========================================================
        // RETURN RESULT
        // ========================================================
        //
        // The serverId/serverUrl are extremely important.
        //
        // Both Render servers share Redis, but they DO NOT share
        // their local /downloads directory.
        //
        // Therefore the frontend needs to know which server owns
        // the completed file.
        //

        return {
          filePath:
            finalPath,

          title,

          serverId:
            SERVER_ID,

          serverUrl:
            SERVER_PUBLIC_URL,
        };
      } catch (err) {
        // ========================================================
        // CLEAN CURRENT ATTEMPT FILES
        // ========================================================

        for (
          const filePath of
            attemptTempFiles
        ) {
          safeUnlink(
            filePath
          );
        }

        // ========================================================
        // CANCELLATION
        // ========================================================

        if (
          cancelled ||
          err instanceof
            UnrecoverableError
        ) {
          safeUnlink(
            rawPath
          );

          safeUnlink(
            videoOnlyPath
          );

          safeUnlink(
            audioOnlyPath
          );

          throw new UnrecoverableError(
            'Job was cancelled by user'
          );
        }

        // ========================================================
        // PROCESS KILLED
        // ========================================================

        if (
          err.message ===
          'PROCESS_KILLED'
        ) {
          /*
           * IMPORTANT:
           *
           * Do NOT delete the stable download files.
           *
           * yt-dlp can resume them on the next attempt.
           */

          throw new Error(
            'A processing step was interrupted unexpectedly'
          );
        }

        // ========================================================
        // NORMAL RETRYABLE ERROR
        // ========================================================

        /*
         * Do NOT delete:
         *
         * rawPath
         * videoOnlyPath
         * audioOnlyPath
         *
         * These files are intentionally preserved so yt-dlp
         * can continue downloading on the next attempt.
         */

        throw err;
      } finally {
        clearInterval(
          cancelCheckInterval
        );
      }
    },

    {
      connection,

      /*
       * yt-dlp + FFmpeg can take a while.
       */
      lockDuration:
        10 * 60 * 1000,

      /*
       * One job per server.
       *
       * Because BOTH Render servers use the same Redis queue,
       * you effectively have:
       *
       * Server A -> 1 concurrent job
       * Server B -> 1 concurrent job
       *
       * Total -> 2 concurrent jobs.
       */
      concurrency: 1,
    }
  );

// ================================================================
// WORKER EVENTS
// ================================================================

worker.on(
  'completed',
  (job) => {
    console.log(
      `Job ${job.id} completed ` +
        `(attempts: ${
          job.attemptsMade + 1
        }) ` +
        `on server ${SERVER_ID}`
    );
  }
);

worker.on(
  'failed',
  (job, err) => {
    console.error(
      `Job ${job?.id} failed after ${
        job
          ? job.attemptsMade + 1
          : '?'
      } attempt(s) on server ${SERVER_ID}:`,
      err.message
    );
  }
);

worker.on(
  'error',
  (err) => {
    console.error(
      '[Worker] internal error:',
      err.message
    );
  }
);

worker.on(
  'stalled',
  (jobId) => {
    console.warn(
      `[Worker] job ${jobId} stalled on server ${SERVER_ID}`
    );
  }
);

worker.on(
  'active',
  (job) => {
    console.log(
      `[Worker] picked up job ${job.id} on server ${SERVER_ID}`
    );

    /*
     * Immediately tell Redis which server owns this job.
     *
     * This is especially useful while the job is downloading.
     */
    job
      .updateProgress({
        stage:
          job.progress?.stage ||
          'starting',
        percent:
          job.progress?.percent ||
          0,
        attempt:
          job.attemptsMade + 1,
        maxAttempts:
          MAX_ATTEMPTS,
        serverId:
          SERVER_ID,
        serverUrl:
          SERVER_PUBLIC_URL,
      })
      .catch(() => {});
  }
);

console.log(
  `Worker started, waiting for jobs... | server=${SERVER_ID} | url=${SERVER_PUBLIC_URL}`
);
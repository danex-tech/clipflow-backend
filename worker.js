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
  toSeconds,
} = require('./utils/ffmpeg');

const DOWNLOADS_DIR = path.join(
  __dirname,
  'downloads'
);

if (!fs.existsSync(DOWNLOADS_DIR)) {
  fs.mkdirSync(DOWNLOADS_DIR, {
    recursive: true,
  });
}

const MAX_ATTEMPTS = 5;

function needsAudioSafety(url) {
  return /youtube\.com|youtu\.be|tiktok\.com/i.test(
    url
  );
}

function makeStageReporter(
  job,
  {
    rangeStart,
    rangeEnd,
    stage,
    attempt,
    maxAttempts,
    expectedSize = null,
  }
) {
  let lastSent = -1;

  return (subPercent, details = null) => {
    const safeSubPercent = Math.min(
      100,
      Math.max(
        0,
        Number.isFinite(subPercent)
          ? subPercent
          : 0
      )
    );

    const overall = Math.round(
      rangeStart +
        (safeSubPercent / 100) *
          (rangeEnd - rangeStart)
    );

    if (
      overall === lastSent &&
      !details
    ) {
      return;
    }

    lastSent = overall;

    const progress = {
      stage,
      percent: overall,
      attempt,
      maxAttempts,
    };

    if (
      details &&
      Number.isFinite(
        details.downloadedBytes
      ) &&
      details.downloadedBytes >= 0
    ) {
      progress.downloadedBytes =
        details.downloadedBytes;
    }

    if (
      details &&
      Number.isFinite(
        details.totalBytes
      ) &&
      details.totalBytes > 0
    ) {
      progress.totalBytes =
        details.totalBytes;
    }

    if (
      details &&
      Number.isFinite(details.speed) &&
      details.speed >= 0
    ) {
      progress.speed =
        details.speed;
    }

    if (
      Number.isFinite(expectedSize) &&
      expectedSize > 0
    ) {
      progress.expectedSize =
        expectedSize;

      if (
        !Number.isFinite(
          progress.totalBytes
        ) ||
        progress.totalBytes <= 0
      ) {
        progress.totalBytes =
          expectedSize;
      }
    }

    job
      .updateProgress(progress)
      .catch(() => {});
  };
}

function safeUnlink(filePath) {
  if (!filePath) return;

  fs.unlink(
    filePath,
    () => {}
  );
}

const worker = new Worker(
  'video-downloads',
  async (job) => {
    const {
      url,
      formatId,
      height,
      hasAudio,
      startTime,
      endTime,
      fileId,
      title,
      duration,
      expectedSize,
    } = job.data;

    const wantsTrim =
      Boolean(startTime || endTime);

    const audioCodec =
      needsAudioSafety(url)
        ? 'aac'
        : 'copy';

    const attempt =
      job.attemptsMade + 1;

    const runId =
      `${fileId}-a${attempt}`;

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

    const finalPath =
      path.join(
        DOWNLOADS_DIR,
        `${fileId}-final.mp4`
      );

    await connection
      .del(`cancel:${job.id}`)
      .catch(() => {});

    let cancelled = false;

    /*
     * Holds the currently running yt-dlp
     * or FFmpeg child process.
     *
     * This allows the cancellation watcher
     * to terminate the active process.
     */
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
              cancelled = true;

              if (
                currentProcess.current
              ) {
                currentProcess.current.kill(
                  'SIGKILL'
                );
              }
            }
          } catch {
            // Ignore transient Redis errors.
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

    const attemptTempFiles = [];

    try {
      /*
       * ============================================================
       * RETRY / RECONNECTING
       * ============================================================
       */
      if (attempt > 1) {
        await job.updateProgress({
          stage: 'Reconnecting',
          percent: 0,
          attempt,
          maxAttempts:
            MAX_ATTEMPTS,
          weakConnection: true,

          ...(Number.isFinite(
            expectedSize
          ) &&
          expectedSize > 0
            ? {
                expectedSize,
                totalBytes:
                  expectedSize,
              }
            : {}),
        });
      }

      /*
       * ============================================================
       * PREPARING
       * ============================================================
       */
      await job.updateProgress({
        stage: 'Preparing video',
        percent: 5,
        attempt,
        maxAttempts:
          MAX_ATTEMPTS,

        ...(Number.isFinite(
          expectedSize
        ) &&
        expectedSize > 0
          ? {
              expectedSize,
              totalBytes:
                expectedSize,
              downloadedBytes: 0,
            }
          : {}),
      });

      /*
       * ============================================================
       * NON-TRIMMED VIDEO
       * ============================================================
       *
       * Preparing:       0–5%
       * Fetching:        5–70%
       * Processing:      70–95%
       * Finalizing:      95–99%
       * Complete:        100%
       */
      if (!wantsTrim) {
        attemptTempFiles.push(
          finalPath
        );

        const reportFetching =
          makeStageReporter(
            job,
            {
              rangeStart: 5,
              rangeEnd: 70,
              stage: 'Fetching video',
              attempt,
              maxAttempts:
                MAX_ATTEMPTS,
              expectedSize,
            }
          );

        await job.updateProgress({
          stage: 'Fetching video',
          percent: 5,
          attempt,
          maxAttempts:
            MAX_ATTEMPTS,

          ...(Number.isFinite(
            expectedSize
          ) &&
          expectedSize > 0
            ? {
                expectedSize,
                totalBytes:
                  expectedSize,
                downloadedBytes: 0,
              }
            : {}),
        });

        await downloadCombined({
          url,
          formatId,
          height,
          hasAudio,
          outputPath: rawPath,

          processRef:
            currentProcess,

          onProgress:
            (percent) => {
              reportFetching(
                percent
              );
            },

          onProgressDetails:
            (details) => {
              reportFetching(
                Number.isFinite(
                  details?.percent
                )
                  ? details.percent
                  : 0,
                details
              );
            },
        });

        throwIfCancelled();

        /*
         * downloadCombined() may perform
         * internal format merging.
         *
         * We therefore expose the remaining
         * work as Processing rather than
         * leaving the UI stuck on Fetching.
         */
        await job.updateProgress({
          stage: 'Processing video',
          percent: 70,
          attempt,
          maxAttempts:
            MAX_ATTEMPTS,

          ...(Number.isFinite(
            expectedSize
          ) &&
          expectedSize > 0
            ? {
                expectedSize,
              }
            : {}),
        });

        throwIfCancelled();

        /*
         * Finalizing
         */
        await job.updateProgress({
          stage: 'Finalizing video',
          percent: 95,
          attempt,
          maxAttempts:
            MAX_ATTEMPTS,

          ...(Number.isFinite(
            expectedSize
          ) &&
          expectedSize > 0
            ? {
                expectedSize,
              }
            : {}),
        });

        fs.renameSync(
          rawPath,
          finalPath
        );

        throwIfCancelled();

        /*
         * Explicit 99% state.
         *
         * 100% is reserved for the
         * completed job below.
         */
        await job.updateProgress({
          stage: 'Finalizing video',
          percent: 99,
          attempt,
          maxAttempts:
            MAX_ATTEMPTS,
        });
      }

      /*
       * ============================================================
       * TRIMMED VIDEO WITH AUDIO INCLUDED IN SOURCE
       * ============================================================
       *
       * Preparing:       0–5%
       * Fetching:        5–65%
       * Trimming:        65–80%
       * Processing:      80–95%
       * Finalizing:      95–99%
       * Complete:        100%
       */
      else if (hasAudio) {
        attemptTempFiles.push(
          finalPath
        );

        const reportFetching =
          makeStageReporter(
            job,
            {
              rangeStart: 5,
              rangeEnd: 65,
              stage: 'Fetching video',
              attempt,
              maxAttempts:
                MAX_ATTEMPTS,
              expectedSize,
            }
          );

        await job.updateProgress({
          stage: 'Fetching video',
          percent: 5,
          attempt,
          maxAttempts:
            MAX_ATTEMPTS,

          ...(Number.isFinite(
            expectedSize
          ) &&
          expectedSize > 0
            ? {
                expectedSize,
                totalBytes:
                  expectedSize,
                downloadedBytes: 0,
              }
            : {}),
        });

        await downloadSingleFormat({
          url,
          formatId,
          outputPath: rawPath,

          processRef:
            currentProcess,

          onProgress:
            (percent) => {
              reportFetching(
                percent
              );
            },

          onProgressDetails:
            (details) => {
              reportFetching(
                Number.isFinite(
                  details?.percent
                )
                  ? details.percent
                  : 0,
                details
              );
            },
        });

        throwIfCancelled();

        /*
         * Trimming
         */
        const reportTrim =
          makeStageReporter(
            job,
            {
              rangeStart: 65,
              rangeEnd: 80,
              stage: 'Trimming video',
              attempt,
              maxAttempts:
                MAX_ATTEMPTS,
            }
          );

        await trimStream({
          inputPath: rawPath,
          outputPath: finalPath,
          startTime,
          endTime,
          videoCodec: 'copy',
          audioCodec,

          processRef:
            currentProcess,

          onProgress:
            reportTrim,
        });

        throwIfCancelled();

        safeUnlink(
          rawPath
        );

        /*
         * Processing
         */
        await job.updateProgress({
          stage: 'Processing video',
          percent: 80,
          attempt,
          maxAttempts:
            MAX_ATTEMPTS,
        });

        throwIfCancelled();

        /*
         * Finalizing
         */
        await job.updateProgress({
          stage: 'Finalizing video',
          percent: 95,
          attempt,
          maxAttempts:
            MAX_ATTEMPTS,
        });

        await job.updateProgress({
          stage: 'Finalizing video',
          percent: 99,
          attempt,
          maxAttempts:
            MAX_ATTEMPTS,
        });
      }

      /*
       * ============================================================
       * TRIMMED VIDEO WITH SEPARATE VIDEO + AUDIO
       * ============================================================
       *
       * Preparing:       0–5%
       * Fetching video:  5–40%
       * Fetching audio:  40–65%
       * Trimming video:  65–72.5%
       * Trimming audio:  72.5–80%
       * Merging video:   80–95%
       * Finalizing:      95–99%
       * Complete:        100%
       */
      else {
        attemptTempFiles.push(
          trimmedVideoPath,
          trimmedAudioPath,
          finalPath
        );

        /*
         * Fetching video
         */
        const reportVideoFetching =
          makeStageReporter(
            job,
            {
              rangeStart: 5,
              rangeEnd: 40,
              stage: 'Fetching video',
              attempt,
              maxAttempts:
                MAX_ATTEMPTS,
              expectedSize,
            }
          );

        await job.updateProgress({
          stage: 'Fetching video',
          percent: 5,
          attempt,
          maxAttempts:
            MAX_ATTEMPTS,

          ...(Number.isFinite(
            expectedSize
          ) &&
          expectedSize > 0
            ? {
                expectedSize,
                totalBytes:
                  expectedSize,
                downloadedBytes: 0,
              }
            : {}),
        });

        await downloadSingleFormat({
          url,
          formatId,
          outputPath:
            videoOnlyPath,

          processRef:
            currentProcess,

          onProgress:
            (percent) => {
              reportVideoFetching(
                percent
              );
            },

          onProgressDetails:
            (details) => {
              reportVideoFetching(
                Number.isFinite(
                  details?.percent
                )
                  ? details.percent
                  : 0,
                details
              );
            },
        });

        throwIfCancelled();

        /*
         * Fetching audio
         */
        const reportAudioFetching =
          makeStageReporter(
            job,
            {
              rangeStart: 40,
              rangeEnd: 65,
              stage: 'Fetching audio',
              attempt,
              maxAttempts:
                MAX_ATTEMPTS,
              expectedSize,
            }
          );

        await downloadBestAudio({
          url,
          outputPath:
            audioOnlyPath,

          processRef:
            currentProcess,

          onProgress:
            (percent) => {
              reportAudioFetching(
                percent
              );
            },

          onProgressDetails:
            (details) => {
              reportAudioFetching(
                Number.isFinite(
                  details?.percent
                )
                  ? details.percent
                  : 0,
                details
              );
            },
        });

        throwIfCancelled();

        /*
         * Trimming video
         */
        const reportVideoTrim =
          makeStageReporter(
            job,
            {
              rangeStart: 65,
              rangeEnd: 72.5,
              stage: 'Trimming video',
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
          videoCodec: 'copy',
          audioCodec: 'copy',

          processRef:
            currentProcess,

          onProgress:
            reportVideoTrim,
        });

        throwIfCancelled();

        safeUnlink(
          videoOnlyPath
        );

        /*
         * Trimming audio
         */
        const reportAudioTrim =
          makeStageReporter(
            job,
            {
              rangeStart: 72.5,
              rangeEnd: 80,
              stage: 'Trimming audio',
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
          videoCodec: 'copy',
          audioCodec,

          processRef:
            currentProcess,

          onProgress:
            reportAudioTrim,
        });

        throwIfCancelled();

        safeUnlink(
          audioOnlyPath
        );

        /*
         * ========================================================
         * MERGING
         * ========================================================
         *
         * The FFmpeg merge progress is mapped:
         *
         * FFmpeg: 0–100%
         * Worker:  80–95%
         *
         * This prevents the UI from appearing stuck at 80%.
         */
        const startSec =
          toSeconds(startTime) || 0;

        const endSec =
          toSeconds(endTime);

        let mergeDurationSec =
          null;

        if (
          Number.isFinite(endSec) &&
          endSec > startSec
        ) {
          mergeDurationSec =
            endSec - startSec;
        } else if (
          Number.isFinite(
            Number(duration)
          ) &&
          Number(duration) > startSec
        ) {
          mergeDurationSec =
            Number(duration) -
            startSec;
        }

        const reportMerge =
          makeStageReporter(
            job,
            {
              rangeStart: 80,
              rangeEnd: 95,
              stage: 'Merging video',
              attempt,
              maxAttempts:
                MAX_ATTEMPTS,
            }
          );

        await job.updateProgress({
          stage: 'Merging video',
          percent: 80,
          attempt,
          maxAttempts:
            MAX_ATTEMPTS,
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

          totalDurationSec:
            mergeDurationSec,

          onProgress:
            reportMerge,
        });

        throwIfCancelled();

        safeUnlink(
          trimmedVideoPath
        );

        safeUnlink(
          trimmedAudioPath
        );

        /*
         * Finalizing
         */
        await job.updateProgress({
          stage: 'Finalizing video',
          percent: 95,
          attempt,
          maxAttempts:
            MAX_ATTEMPTS,
        });

        await job.updateProgress({
          stage: 'Finalizing video',
          percent: 99,
          attempt,
          maxAttempts:
            MAX_ATTEMPTS,
        });
      }

      /*
       * ============================================================
       * DIAGNOSTIC CHECK
       * ============================================================
       */
      console.log(
        `[Worker] Final file check | path=${finalPath} | exists=${fs.existsSync(
          finalPath
        )}`
      );

      if (!fs.existsSync(finalPath)) {
        throw new Error(
          'Final video file was not created'
        );
      }

      /*
       * ============================================================
       * COMPLETE
       * ============================================================
       *
       * 100% is emitted only after the final
       * file has actually been confirmed.
       */
      await job.updateProgress({
        stage: 'Complete',
        percent: 100,
        attempt,
        maxAttempts:
          MAX_ATTEMPTS,

        ...(Number.isFinite(
          expectedSize
        ) &&
        expectedSize > 0
          ? {
              expectedSize,
            }
          : {}),
      });

      return {
        filePath: finalPath,
        title,
      };
    } catch (err) {
      for (
        const filePath of
        attemptTempFiles
      ) {
        safeUnlink(filePath);
      }

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

        safeUnlink(
          trimmedVideoPath
        );

        safeUnlink(
          trimmedAudioPath
        );

        safeUnlink(
          finalPath
        );

        throw new UnrecoverableError(
          'Job was cancelled by user'
        );
      }

      if (
        err.message ===
        'PROCESS_KILLED'
      ) {
        throw new Error(
          'A processing step was interrupted unexpectedly'
        );
      }

      throw err;
    } finally {
      clearInterval(
        cancelCheckInterval
      );

      if (
        currentProcess.current
      ) {
        currentProcess.current =
          null;
      }
    }
  },
  {
    connection,
    lockDuration:
      10 * 60 * 1000,
    concurrency: 1,
  }
);

worker.on(
  'completed',
  (job) => {
    console.log(
      `Job ${job.id} completed (attempts: ${
        job.attemptsMade + 1
      })`
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
      } attempt(s):`,
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
      `[Worker] job ${jobId} stalled`
    );
  }
);

worker.on(
  'active',
  (job) => {
    console.log(
      `[Worker] picked up job ${job.id}`
    );
  }
);

console.log(
  'Worker started, waiting for jobs...'
);

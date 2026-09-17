
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
} = require('./utils/ffmpeg');

const DOWNLOADS_DIR = path.join(__dirname, 'downloads');

if (!fs.existsSync(DOWNLOADS_DIR)) {
  fs.mkdirSync(DOWNLOADS_DIR, { recursive: true });
}

const MAX_ATTEMPTS = 5;

function needsAudioSafety(url) {
  return /youtube\.com|youtu\.be|tiktok\.com/i.test(url);
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
      Number.isFinite(
        details.speed
      ) &&
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
  fs.unlink(filePath, () => {});
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

    const rawPath = path.join(
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

    const finalPath = path.join(
      DOWNLOADS_DIR,
      `${fileId}-final.mp4`
    );

    await connection
      .del(`cancel:${job.id}`)
      .catch(() => {});

    let cancelled = false;

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
      if (attempt > 1) {
        await job.updateProgress({
          stage: 'reconnecting',
          percent: 0,
          attempt,
          maxAttempts: MAX_ATTEMPTS,
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

      if (!wantsTrim) {
        attemptTempFiles.push(
          finalPath
        );

        const reportDownload =
          makeStageReporter(job, {
            rangeStart: 10,
            rangeEnd: 99,
            stage: 'downloading',
            attempt,
            maxAttempts:
              MAX_ATTEMPTS,
            expectedSize,
          });

        await job.updateProgress({
          stage: 'downloading',
          percent: 10,
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

          onProgress: (percent) => {
            reportDownload(
              percent
            );
          },

          onProgressDetails: (
            details
          ) => {
            reportDownload(
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

        fs.renameSync(
          rawPath,
          finalPath
        );

        await job.updateProgress({
          stage: 'downloading',
          percent: 99,
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
      } else if (hasAudio) {
        attemptTempFiles.push(
          finalPath
        );

        const reportDownload =
          makeStageReporter(job, {
            rangeStart: 10,
            rangeEnd: 60,
            stage: 'downloading',
            attempt,
            maxAttempts:
              MAX_ATTEMPTS,
            expectedSize,
          });

        await job.updateProgress({
          stage: 'downloading',
          percent: 10,
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

          onProgress: (percent) => {
            reportDownload(
              percent
            );
          },

          onProgressDetails: (
            details
          ) => {
            reportDownload(
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

        const reportTrim =
          makeStageReporter(job, {
            rangeStart: 60,
            rangeEnd: 99,
            stage: 'trimming',
            attempt,
            maxAttempts:
              MAX_ATTEMPTS,
          });

        await trimStream({
          inputPath: rawPath,
          outputPath: finalPath,
          startTime,
          endTime,
          videoCodec: 'copy',
          audioCodec,
          onProgress:
            reportTrim,
        });

        throwIfCancelled();

        safeUnlink(rawPath);
      } else {
        attemptTempFiles.push(
          trimmedVideoPath,
          trimmedAudioPath,
          finalPath
        );

        const reportVideoDl =
          makeStageReporter(job, {
            rangeStart: 10,
            rangeEnd: 35,
            stage: 'downloading video',
            attempt,
            maxAttempts:
              MAX_ATTEMPTS,
            expectedSize,
          });

        await job.updateProgress({
          stage: 'downloading video',
          percent: 10,
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

          onProgress: (percent) => {
            reportVideoDl(
              percent
            );
          },

          onProgressDetails: (
            details
          ) => {
            reportVideoDl(
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

        const reportAudioDl =
          makeStageReporter(job, {
            rangeStart: 35,
            rangeEnd: 50,
            stage: 'downloading audio',
            attempt,
            maxAttempts:
              MAX_ATTEMPTS,
            expectedSize,
          });

        await job.updateProgress({
          stage: 'downloading audio',
          percent: 35,
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

        await downloadBestAudio({
          url,
          outputPath:
            audioOnlyPath,

          onProgress: (percent) => {
            reportAudioDl(
              percent
            );
          },

          onProgressDetails: (
            details
          ) => {
            reportAudioDl(
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

        const reportVideoTrim =
          makeStageReporter(job, {
            rangeStart: 50,
            rangeEnd: 65,
            stage: 'trimming video',
            attempt,
            maxAttempts:
              MAX_ATTEMPTS,
          });

        await trimStream({
          inputPath:
            videoOnlyPath,
          outputPath:
            trimmedVideoPath,
          startTime,
          endTime,
          videoCodec: 'copy',
          audioCodec: 'copy',
          onProgress:
            reportVideoTrim,
        });

        throwIfCancelled();

        safeUnlink(
          videoOnlyPath
        );

        const reportAudioTrim =
          makeStageReporter(job, {
            rangeStart: 65,
            rangeEnd: 80,
            stage: 'trimming audio',
            attempt,
            maxAttempts:
              MAX_ATTEMPTS,
          });

        await trimStream({
          inputPath:
            audioOnlyPath,
          outputPath:
            trimmedAudioPath,
          startTime,
          endTime,
          videoCodec: 'copy',
          audioCodec,
          onProgress:
            reportAudioTrim,
        });

        throwIfCancelled();

        safeUnlink(
          audioOnlyPath
        );

        await job.updateProgress({
          stage: 'merging',
          percent: 90,
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
        });

        throwIfCancelled();

        safeUnlink(
          trimmedVideoPath
        );

        safeUnlink(
          trimmedAudioPath
        );
      }

      // Diagnostic check: confirm that the final file
      // actually exists immediately after processing.
      console.log(
        `[Worker] Final file check | path=${finalPath} | exists=${fs.existsSync(finalPath)}`
      );

      await job.updateProgress({
        stage: 'done',
        percent: 100,
        attempt,
        maxAttempts: MAX_ATTEMPTS,
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
        safeUnlink(rawPath);
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

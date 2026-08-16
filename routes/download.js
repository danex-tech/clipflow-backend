    const express = require('express');
const fs = require('fs');
const crypto = require('crypto');
const { Job } = require('bullmq');

const downloadQueue = require('../utils/downloadQueue');
const connection = require('../utils/redisConnection');

const router = express.Router();

const MAX_ATTEMPTS = 5;

function sanitizeFilename(title) {
  if (!title) return 'clipflow';

  const cleaned = title
    .replace(/[/\\?%*:|"<>]/g, '')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, 100);

  return cleaned || 'clipflow';
}

// ================================================================
// POST /api/download
// ================================================================

router.post('/download', async (req, res) => {
  try {
    const {
      url,
      formatId,
      hasAudio,
      startTime,
      endTime,
      title,
      duration,
    } = req.body;

    if (!url || !formatId) {
      return res.status(400).json({
        error: 'url and formatId are required',
      });
    }

    // Every download gets its own unique ID.
    // This prevents two simultaneous jobs from sharing files.
    const fileId = crypto.randomBytes(8).toString('hex');

    const job = await downloadQueue.add(
      'download-job',
      {
        url,
        formatId,
        hasAudio: Boolean(hasAudio),
        startTime,
        endTime,
        fileId,
        title,
        duration,
      },
      {
        attempts: MAX_ATTEMPTS,

        // Retry after temporary failures such as
        // dropped connections.
        backoff: {
          type: 'fixed',
          delay: 5000,
        },
      }
    );

    console.log(
      `[Download] Created job ${job.id} | fileId=${fileId}`
    );

    res.json({
      jobId: job.id,
    });
  } catch (err) {
    console.error(
      '[Download] Failed to create job:',
      err.message
    );

    res.status(500).json({
      error: 'Failed to create download job',
    });
  }
});

// ================================================================
// GET /api/download/status/:jobId
// ================================================================

router.get('/download/status/:jobId', async (req, res) => {
  try {
    res.set('Cache-Control', 'no-store');

    const job = await Job.fromId(
      downloadQueue,
      req.params.jobId
    );

    if (!job) {
      return res.status(404).json({
        error: 'Job not found',
      });
    }

    const state = await job.getState();

    const progress =
      job.progress || {
        stage: 'queued',
        percent: 0,
      };

    const response = {
      state,
      progress,
    };

    // ------------------------------------------------------------
    // QUEUE POSITION
    // ------------------------------------------------------------

    if (state === 'waiting') {
      const waitingJobs =
        await downloadQueue.getWaiting();

      const position = waitingJobs.findIndex(
        (j) =>
          String(j.id) ===
          String(req.params.jobId)
      );

      if (position !== -1) {
        response.queuePosition = position;
        response.totalWaiting =
          waitingJobs.length;
      }
    }

    // ------------------------------------------------------------
    // FAILED
    // ------------------------------------------------------------

    if (state === 'failed') {
      const wasCancelled =
        await connection.get(
          `cancel:${req.params.jobId}`
        );

      if (wasCancelled) {
        response.state = 'cancelled';
      } else {
        response.failedReason =
          job.failedReason;

        response.attemptsMade =
          job.attemptsMade;

        response.maxAttempts =
          MAX_ATTEMPTS;
      }
    }

    res.json(response);
  } catch (err) {
    console.error(
      '[Download] Status error:',
      err.message
    );

    res.status(500).json({
      error: 'Failed to get download status',
    });
  }
});

// ================================================================
// POST /api/download/cancel/:jobId
// ================================================================

router.post(
  '/download/cancel/:jobId',
  async (req, res) => {
    try {
      const job = await Job.fromId(
        downloadQueue,
        req.params.jobId
      );

      if (!job) {
        return res.status(404).json({
          error: 'Job not found',
        });
      }

      const state = await job.getState();

      // ----------------------------------------------------------
      // QUEUED JOB
      // ----------------------------------------------------------

      if (
        state === 'waiting' ||
        state === 'delayed'
      ) {
        await job.remove();

        return res.json({
          cancelled: true,
          wasActive: false,
        });
      }

      // ----------------------------------------------------------
      // ACTIVE JOB
      // ----------------------------------------------------------

      await connection.set(
        `cancel:${req.params.jobId}`,
        '1',
        'EX',
        3600
      );

      res.json({
        cancelled: true,
        wasActive: true,
      });
    } catch (err) {
      console.error(
        '[Download] Cancel error:',
        err.message
      );

      res.status(500).json({
        error: 'Failed to cancel download',
      });
    }
  }
);

// ================================================================
// GET /api/download/result/:jobId
// ================================================================
//
// IMPORTANT:
//
// This endpoint sends the already-processed final file.
//
// The file is intentionally NOT deleted after sending.
//
// Why?
//
// Chrome can pause a large download and later issue another
// HTTP Range request to continue from where it stopped.
//
// If we deleted the file after the first request, Chrome's
// resume request would receive ENOENT / file not found.
//
// ================================================================

router.get(
  '/download/result/:jobId',
  async (req, res) => {
    try {
      const job = await Job.fromId(
        downloadQueue,
        req.params.jobId
      );

      if (!job) {
        return res.status(404).json({
          error: 'Job not found',
        });
      }

      const state = await job.getState();

      if (state !== 'completed') {
        return res.status(400).json({
          error:
            `Job is not ready yet (current state: ${state})`,
        });
      }

      if (!job.returnvalue) {
        return res.status(500).json({
          error:
            'Completed job has no result information',
        });
      }

      const {
        filePath,
        title,
      } = job.returnvalue;

      // ----------------------------------------------------------
      // VERIFY FILE STILL EXISTS
      // ----------------------------------------------------------

      if (!filePath) {
        return res.status(500).json({
          error:
            'Completed job does not contain a file path',
        });
      }

      if (!fs.existsSync(filePath)) {
        console.error(
          `[Download] Final file missing for job ${job.id}: ${filePath}`
        );

        return res.status(404).json({
          error:
            'The processed video file is no longer available on the server',
        });
      }

      const stats = fs.statSync(filePath);

      if (!stats.isFile()) {
        return res.status(500).json({
          error:
            'The processed output is not a valid file',
        });
      }

      // ----------------------------------------------------------
      // DOWNLOAD NAME
      // ----------------------------------------------------------

      const downloadName =
        `${sanitizeFilename(title)}.mp4`;

      // ----------------------------------------------------------
      // IMPORTANT RANGE SUPPORT
      // ----------------------------------------------------------
      //
      // Express/sendFile supports HTTP Range requests.
      //
      // Chrome can therefore do something like:
      //
      // Range: bytes=334000000-
      //
      // when continuing a paused download.
      //
      // We explicitly keep acceptRanges enabled.
      // ----------------------------------------------------------

      res.download(
        filePath,
        downloadName,
        {
          acceptRanges: true,
          lastModified: true,
          cacheControl: false,
        },
        (err) => {
          if (err) {
            console.error(
              `[Download] Error sending file for job ${job.id}:`,
              err.message
            );

            /*
             * DO NOT DELETE THE FILE HERE.
             *
             * If the browser paused the download, disconnected,
             * or needs to resume later, the file must remain.
             */
            return;
          }

          console.log(
            `[Download] File successfully served for job ${job.id} | ` +
            `${stats.size} bytes`
          );

          /*
           * VERY IMPORTANT:
           *
           * There is intentionally NO:
           *
           * fs.unlink(filePath)
           *
           * here.
           *
           * The processed file stays on disk.
           */
        }
      );
    } catch (err) {
      console.error(
        '[Download] Result error:',
        err.message
      );

      if (!res.headersSent) {
        res.status(500).json({
          error:
            'Failed to send processed video',
        });
      }
    }
  }
);

module.exports = router;

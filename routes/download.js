const express = require('express');
const path = require('path');
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

// POST /api/download
// Body: { url, formatId, hasAudio, startTime?, endTime?, title?, duration? }
router.post('/download', async (req, res) => {
  const { url, formatId, hasAudio, startTime, endTime, title, duration } = req.body;

  if (!url || !formatId) {
    return res.status(400).json({ error: 'url and formatId are required' });
  }

  const fileId = crypto.randomBytes(8).toString('hex');

  const job = await downloadQueue.add(
    'download-job',
    { url, formatId, hasAudio: Boolean(hasAudio), startTime, endTime, fileId, title, duration },
    {
      // If a step fails (most commonly a dropped connection mid-download),
      // automatically retry instead of just giving up. Fixed 5s gap between
      // tries avoids hammering the network right after it drops.
      attempts: MAX_ATTEMPTS,
      backoff: { type: 'fixed', delay: 5000 },
    }
  );

  res.json({ jobId: job.id });
});

// GET /api/download/status/:jobId
router.get('/download/status/:jobId', async (req, res) => {
  res.set('Cache-Control', 'no-store');

  const job = await Job.fromId(downloadQueue, req.params.jobId);

  if (!job) {
    return res.status(404).json({ error: 'Job not found' });
  }

  let state = await job.getState();
  const progress = job.progress || { stage: 'queued', percent: 0 };

  const response = { state, progress };

  if (state === 'failed') {
    const wasCancelled = await connection.get(`cancel:${req.params.jobId}`);
    if (wasCancelled) {
      response.state = 'cancelled';
    } else {
      response.failedReason = job.failedReason;
      response.attemptsMade = job.attemptsMade;
      response.maxAttempts = MAX_ATTEMPTS;
    }
  }

  res.json(response);
});

// POST /api/download/cancel/:jobId
// Stops a job that's queued or actively processing. Queued jobs are
// removed immediately; active jobs get a "please stop" flag that the
// worker checks every couple of seconds.
router.post('/download/cancel/:jobId', async (req, res) => {
  const job = await Job.fromId(downloadQueue, req.params.jobId);

  if (!job) {
    return res.status(404).json({ error: 'Job not found' });
  }

  const state = await job.getState();

  if (state === 'waiting' || state === 'delayed') {
    await job.remove();
    return res.json({ cancelled: true, wasActive: false });
  }

  // Flag it for the worker to notice and terminate. Expires after an hour
  // as a safety net so old flags don't linger in Redis forever.
  await connection.set(`cancel:${req.params.jobId}`, '1', 'EX', 3600);
  res.json({ cancelled: true, wasActive: true });
});

// GET /api/download/result/:jobId
router.get('/download/result/:jobId', async (req, res) => {
  const job = await Job.fromId(downloadQueue, req.params.jobId);

  if (!job) {
    return res.status(404).json({ error: 'Job not found' });
  }

  const state = await job.getState();
  if (state !== 'completed') {
    return res.status(400).json({ error: `Job is not ready yet (current state: ${state})` });
  }

  const { filePath, title } = job.returnvalue;
  const downloadName = `${sanitizeFilename(title)}.mp4`;

  res.download(filePath, downloadName, (err) => {
    if (!err) {
      // Only clean up once the file has actually reached the user. If the
      // transfer failed or was interrupted (e.g. a dropped connection),
      // we deliberately leave the file in place so clicking "Save file"
      // again can succeed instead of finding nothing there.
      fs.unlink(filePath, () => {});
    } else {
      console.error('Error sending file (file kept for retry):', err.message);
    }
  });
});

module.exports = router;    {
      // If a step fails (most commonly a dropped connection mid-download),
      // automatically retry instead of just giving up. Fixed 5s gap between
      // tries avoids hammering the network right after it drops.
      attempts: MAX_ATTEMPTS,
      backoff: { type: 'fixed', delay: 5000 },
    }
  );

  res.json({ jobId: job.id });
});

// GET /api/download/status/:jobId
router.get('/download/status/:jobId', async (req, res) => {
  res.set('Cache-Control', 'no-store');

  const job = await Job.fromId(downloadQueue, req.params.jobId);

  if (!job) {
    return res.status(404).json({ error: 'Job not found' });
  }

  let state = await job.getState();
  const progress = job.progress || { stage: 'queued', percent: 0 };

  const response = { state, progress };

  if (state === 'waiting') {
    // How many jobs are ahead of this one, so the frontend can show
    // something like "3 jobs ahead of you" instead of a silent wait.
    const waitingJobs = await downloadQueue.getWaiting();
    const position = waitingJobs.findIndex((j) => String(j.id) === String(req.params.jobId));
    if (position !== -1) {
      response.queuePosition = position; // 0 = next in line
      response.totalWaiting = waitingJobs.length;
    }
  }

  if (state === 'failed') {
    const wasCancelled = await connection.get(`cancel:${req.params.jobId}`);
    if (wasCancelled) {
      response.state = 'cancelled';
    } else {
      response.failedReason = job.failedReason;
      response.attemptsMade = job.attemptsMade;
      response.maxAttempts = MAX_ATTEMPTS;
    }
  }

  res.json(response);
});

// POST /api/download/cancel/:jobId
// Stops a job that's queued or actively processing. Queued jobs are
// removed immediately; active jobs get a "please stop" flag that the
// worker checks every couple of seconds.
router.post('/download/cancel/:jobId', async (req, res) => {
  const job = await Job.fromId(downloadQueue, req.params.jobId);

  if (!job) {
    return res.status(404).json({ error: 'Job not found' });
  }

  const state = await job.getState();

  if (state === 'waiting' || state === 'delayed') {
    await job.remove();
    return res.json({ cancelled: true, wasActive: false });
  }

  // Flag it for the worker to notice and terminate. Expires after an hour
  // as a safety net so old flags don't linger in Redis forever.
  await connection.set(`cancel:${req.params.jobId}`, '1', 'EX', 3600);
  res.json({ cancelled: true, wasActive: true });
});

// GET /api/download/result/:jobId
router.get('/download/result/:jobId', async (req, res) => {
  const job = await Job.fromId(downloadQueue, req.params.jobId);

  if (!job) {
    return res.status(404).json({ error: 'Job not found' });
  }

  const state = await job.getState();
  if (state !== 'completed') {
    return res.status(400).json({ error: `Job is not ready yet (current state: ${state})` });
  }

  const { filePath, title } = job.returnvalue;
  const downloadName = `${sanitizeFilename(title)}.mp4`;

  res.download(filePath, downloadName, (err) => {
    if (!err) {
      // Only clean up once the file has actually reached the user. If the
      // transfer failed or was interrupted (e.g. a dropped connection),
      // we deliberately leave the file in place so clicking "Save file"
      // again can succeed instead of finding nothing there.
      fs.unlink(filePath, () => {});
    } else {
      console.error('Error sending file (file kept for retry):', err.message);
    }
  });
});

module.exports = router;
    {
      // If a step fails (most commonly a dropped connection mid-download),
      // automatically retry instead of just giving up. Fixed 5s gap between
      // tries avoids hammering the network right after it drops.
      attempts: MAX_ATTEMPTS,
      backoff: { type: 'fixed', delay: 5000 },
    }
  );

  res.json({ jobId: job.id });
});

// GET /api/download/status/:jobId
router.get('/download/status/:jobId', async (req, res) => {
  res.set('Cache-Control', 'no-store');

  const job = await Job.fromId(downloadQueue, req.params.jobId);

  if (!job) {
    return res.status(404).json({ error: 'Job not found' });
  }

  let state = await job.getState();
  const progress = job.progress || { stage: 'queued', percent: 0 };

  const response = { state, progress };

  if (state === 'failed') {
    const wasCancelled = await connection.get(`cancel:${req.params.jobId}`);
    if (wasCancelled) {
      response.state = 'cancelled';
    } else {
      response.failedReason = job.failedReason;
      response.attemptsMade = job.attemptsMade;
      response.maxAttempts = MAX_ATTEMPTS;
    }
  }

  res.json(response);
});

// POST /api/download/cancel/:jobId
// Stops a job that's queued or actively processing. Queued jobs are
// removed immediately; active jobs get a "please stop" flag that the
// worker checks every couple of seconds.
router.post('/download/cancel/:jobId', async (req, res) => {
  const job = await Job.fromId(downloadQueue, req.params.jobId);

  if (!job) {
    return res.status(404).json({ error: 'Job not found' });
  }

  const state = await job.getState();

  if (state === 'waiting' || state === 'delayed') {
    await job.remove();
    return res.json({ cancelled: true, wasActive: false });
  }

  // Flag it for the worker to notice and terminate. Expires after an hour
  // as a safety net so old flags don't linger in Redis forever.
  await connection.set(`cancel:${req.params.jobId}`, '1', 'EX', 3600);
  res.json({ cancelled: true, wasActive: true });
});

// GET /api/download/result/:jobId
router.get('/download/result/:jobId', async (req, res) => {
  const job = await Job.fromId(downloadQueue, req.params.jobId);

  if (!job) {
    return res.status(404).json({ error: 'Job not found' });
  }

  const state = await job.getState();
  if (state !== 'completed') {
    return res.status(400).json({ error: `Job is not ready yet (current state: ${state})` });
  }

  const { filePath, title } = job.returnvalue;
  const downloadName = `${sanitizeFilename(title)}.mp4`;

  res.download(filePath, downloadName, (err) => {
    if (!err) {
      // Only clean up once the file has actually reached the user. If the
      // transfer failed or was interrupted (e.g. a dropped connection),
      // we deliberately leave the file in place so clicking "Save file"
      // again can succeed instead of finding nothing there.
      fs.unlink(filePath, () => {});
    } else {
      console.error('Error sending file (file kept for retry):', err.message);
    }
  });
});

module.exports = router;

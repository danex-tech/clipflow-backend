const { Queue } = require('bullmq');
const connection = require('./redisConnection');

// This is the "notepad" — jobs get added here, and a separate worker
// process picks them up and works through them one at a time.
const downloadQueue = new Queue('video-downloads', { connection });

module.exports = downloadQueue;
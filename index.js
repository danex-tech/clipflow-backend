require('dotenv').config();

const express = require('express');
const cors = require('cors');

const videoInfoRoute = require('./routes/videoInfo');
const downloadRoute = require('./routes/download');

const {
  startCleanupScheduler,
} = require('./utils/cleanupScheduler');

const app = express();

const SERVER_ID =
  process.env.SERVER_ID ||
  process.env.QUEUE_NAME ||
  'unknown-server';

const QUEUE_NAME =
  process.env.QUEUE_NAME ||
  'video-downloads';

app.use(cors());

app.use(express.json());

app.get('/', (req, res) => {
  res.json({
    status: 'ok',
    message: 'Video downloader backend is running',
    serverId: SERVER_ID,
    queueName: QUEUE_NAME,
  });
});

app.get('/health', (req, res) => {
  res.json({
    status: 'healthy',
    serverId: SERVER_ID,
    queueName: QUEUE_NAME,
  });
});

app.use('/api', videoInfoRoute);

app.use('/api', downloadRoute);

const PORT =
  process.env.PORT || 5000;

app.listen(PORT, () => {
  console.log(
    `Server running on port ${PORT}`
  );

  console.log(
    `[Server] ID: ${SERVER_ID}`
  );

  console.log(
    `[Server] Queue: ${QUEUE_NAME}`
  );
});

startCleanupScheduler();
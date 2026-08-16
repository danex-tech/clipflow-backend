require('dotenv').config();
const express = require('express');
const cors = require('cors');
const videoInfoRoute = require('./routes/videoInfo');
const downloadRoute = require('./routes/download');
const { startCleanupScheduler } = require('./utils/cleanupScheduler');

const app = express();

app.use(cors());
app.use(express.json());

app.get('/', (req, res) => {
  res.json({ status: 'ok', message: 'Video downloader backend is running' });
});

app.get('/health', (req, res) => {
  res.json({ status: 'healthy' });
});

app.use('/api', videoInfoRoute);
app.use('/api', downloadRoute);

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});

startCleanupScheduler();
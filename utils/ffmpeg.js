const { spawn } = require('child_process');

const FFMPEG_PATH = process.env.FFMPEG_PATH || 'ffmpeg';

/**
 * Converts:
 *
 * HH:MM:SS
 * MM:SS
 * plain seconds
 *
 * into total seconds.
 */
function toSeconds(time) {
  if (
    time === undefined ||
    time === null ||
    time === ''
  ) {
    return null;
  }

  if (typeof time === 'number') {
    return time;
  }

  const parts = String(time)
    .split(':')
    .map(Number);

  if (parts.some(isNaN)) {
    return null;
  }

  if (parts.length === 3) {
    const [h, m, s] = parts;

    return (
      h * 3600 +
      m * 60 +
      s
    );
  }

  if (parts.length === 2) {
    const [m, s] = parts;

    return (
      m * 60 +
      s
    );
  }

  return parts[0];
}

/**
 * Parses FFmpeg's machine-readable:
 *
 * -progress pipe:1
 *
 * output.
 *
 * FFmpeg continuously writes key=value pairs such as:
 *
 * out_time_ms=1234567
 *
 * IMPORTANT:
 *
 * We ALWAYS consume stdout.
 *
 * If stdout is not drained, the pipe can eventually fill up and
 * FFmpeg can block while waiting for the pipe to be read.
 */
function watchFfmpegProgress(
  proc,
  totalDurationSec,
  onProgress
) {
  let buffer = '';

  proc.stdout.on('data', (chunk) => {
    buffer += chunk.toString();

    const lines = buffer.split('\n');

    buffer = lines.pop();

    /*
     * Even when there is no progress callback, stdout still gets
     * consumed because this listener is attached.
     */
    if (!onProgress || !totalDurationSec) {
      return;
    }

    for (const line of lines) {
      const match =
        line.match(/out_time_ms=(\d+)/);

      if (!match) {
        continue;
      }

      /*
       * FFmpeg's out_time_ms value is expressed in
       * microseconds despite the historical variable name.
       */
      const elapsedSec =
        parseInt(match[1], 10) / 1_000_000;

      const percent = Math.min(
        100,
        (elapsedSec / totalDurationSec) * 100
      );

      onProgress(percent);
    }
  });
}

/**
 * Trims a file to:
 *
 * startTime -> endTime
 *
 * The default behavior uses stream copy:
 *
 *   video: copy
 *   audio: copy
 *
 * which is much faster than re-encoding.
 *
 * For sources where the audio needs compatibility conversion,
 * audioCodec can be set to "aac".
 */
function trimStream({
  inputPath,
  outputPath,
  startTime,
  endTime,
  videoCodec = 'copy',
  audioCodec = 'copy',
  processRef,
  onProgress,
}) {
  return new Promise((resolve, reject) => {
    const startSec =
      toSeconds(startTime) || 0;

    const endSec =
      toSeconds(endTime);

    if (
      endSec === null ||
      endSec - startSec <= 0
    ) {
      reject(
        new Error(
          'endTime must be after startTime'
        )
      );

      return;
    }

    const clipDuration =
      endSec - startSec;

    /*
     * We place -ss after -i when using stream copy.
     *
     * This is slower than input seeking, but it gives more
     * predictable trimming behavior with copied streams.
     */
    const args = [
      '-y',
      '-i',
      inputPath,
    ];

    if (startSec > 0) {
      args.push(
        '-ss',
        String(startSec)
      );
    }

    args.push(
      '-t',
      String(clipDuration)
    );

    args.push(
      '-c:v',
      videoCodec,
      '-c:a',
      audioCodec
    );

    /*
     * Only specify an AAC bitrate when we are actually
     * encoding audio to AAC.
     */
    if (audioCodec === 'aac') {
      args.push(
        '-b:a',
        '192k'
      );
    }

    /*
     * Machine-readable progress output.
     */
    args.push(
      '-progress',
      'pipe:1',
      '-nostats'
    );

    args.push(outputPath);

    const proc = spawn(
      FFMPEG_PATH,
      args
    );

    if (processRef) {
      processRef.current = proc;
    }

    /*
     * Always consume stdout.
     */
    watchFfmpegProgress(
      proc,
      clipDuration,
      onProgress
    );

    let stderr = '';

    proc.stderr.on('data', (chunk) => {
      stderr += chunk.toString();
    });

    proc.on('error', (err) => {
      if (processRef) {
        processRef.current = null;
      }

      reject(
        new Error(
          `Failed to start ffmpeg: ${err.message}`
        )
      );
    });

    proc.on('close', (code, signal) => {
      if (processRef) {
        processRef.current = null;
      }

      if (signal === 'SIGKILL') {
        reject(
          new Error('PROCESS_KILLED')
        );

        return;
      }

      if (code !== 0) {
        reject(
          new Error(
            stderr ||
              `ffmpeg exited with code ${code}`
          )
        );

        return;
      }

      /*
       * FFmpeg can occasionally finish before the last progress
       * event reaches the callback.
       */
      if (onProgress) {
        onProgress(100);
      }

      resolve(outputPath);
    });
  });
}

/**
 * Merges a separately downloaded video stream and audio stream.
 *
 * Uses:
 *
 *   -c copy
 *
 * so neither stream is re-encoded.
 *
 * This is normally very fast compared with transcoding.
 */
function mergeStreams({
  videoPath,
  audioPath,
  outputPath,
  processRef,
}) {
  return new Promise((resolve, reject) => {
    const args = [
      '-y',

      '-i',
      videoPath,

      '-i',
      audioPath,

      '-c',
      'copy',

      outputPath,
    ];

    const proc = spawn(
      FFMPEG_PATH,
      args
    );

    if (processRef) {
      processRef.current = proc;
    }

    let stderr = '';

    proc.stderr.on('data', (chunk) => {
      stderr += chunk.toString();
    });

    proc.on('error', (err) => {
      if (processRef) {
        processRef.current = null;
      }

      reject(
        new Error(
          `Failed to start ffmpeg: ${err.message}`
        )
      );
    });

    proc.on('close', (code, signal) => {
      if (processRef) {
        processRef.current = null;
      }

      if (signal === 'SIGKILL') {
        reject(
          new Error('PROCESS_KILLED')
        );

        return;
      }

      if (code !== 0) {
        reject(
          new Error(
            stderr ||
              `ffmpeg exited with code ${code}`
          )
        );

        return;
      }

      resolve(outputPath);
    });
  });
}

module.exports = {
  trimStream,
  mergeStreams,
  toSeconds,
};
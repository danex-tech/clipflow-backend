const { spawn } = require('child_process');

const FFMPEG_PATH =
  process.env.FFMPEG_PATH || 'ffmpeg';

const MAX_STDERR_LENGTH = 64 * 1024;

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
 * Prevents stderr from growing without a limit.
 *
 * FFmpeg can produce a large amount of diagnostic output.
 * We only keep the most recent portion because the complete
 * stderr log is not required for normal operation.
 */
function appendLimited(current, chunk) {
  const next = current + chunk;

  if (next.length <= MAX_STDERR_LENGTH) {
    return next;
  }

  return next.slice(
    next.length - MAX_STDERR_LENGTH
  );
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
        Math.max(
          0,
          (elapsedSec / totalDurationSec) * 100
        )
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
      stderr = appendLimited(
        stderr,
        chunk.toString()
      );
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
 * Progress is reported through FFmpeg's machine-readable
 * -progress output.
 */
function mergeStreams({
  videoPath,
  audioPath,
  outputPath,
  processRef,
  totalDurationSec,
  onProgress,
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
    ];

    /*
     * Add machine-readable progress output when a duration
     * and progress callback are available.
     */
    if (onProgress) {
      args.push(
        '-progress',
        'pipe:1',
        '-nostats'
      );
    }

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
     *
     * This is especially important when -progress pipe:1
     * is enabled because FFmpeg continuously writes progress
     * information to stdout.
     */
    watchFfmpegProgress(
      proc,
      totalDurationSec,
      onProgress
    );

    let stderr = '';

    proc.stderr.on('data', (chunk) => {
      stderr = appendLimited(
        stderr,
        chunk.toString()
      );
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

      if (onProgress) {
        onProgress(100);
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
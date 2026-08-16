const { spawn } = require('child_process');

// ================================================================
// CONFIG
// ================================================================

const YTDLP_PATH =
  process.env.YTDLP_PATH || 'yt-dlp';

const COOKIES_BROWSER =
  process.env.YTDLP_COOKIES_BROWSER;

const COOKIES_FILE =
  process.env.YTDLP_COOKIES_FILE;

// ================================================================
// COOKIE ARGUMENTS
// ================================================================

function addCookieArgs(args) {
  if (COOKIES_FILE) {
    args.push(
      '--cookies',
      COOKIES_FILE
    );
  } else if (COOKIES_BROWSER) {
    args.push(
      '--cookies-from-browser',
      COOKIES_BROWSER
    );
  }

  return args;
}

// ================================================================
// PROXY
// ================================================================

function addProxyArgs(args) {
  if (process.env.PROXY_URL) {
    args.push(
      '--proxy',
      process.env.PROXY_URL
    );
  }

  return args;
}

// ================================================================
// EJS
// ================================================================

function addRemoteComponentArgs(args) {
  args.push(
    '--remote-components',
    'ejs:github'
  );

  return args;
}

// ================================================================
// IMPERSONATION
// ================================================================

function addImpersonateArgs(args) {
  args.push(
    '--impersonate',
    'chrome'
  );

  return args;
}

// ================================================================
// PO TOKEN PROVIDER
// ================================================================

function addPotProviderArgs(args) {
  if (process.env.POT_PROVIDER_URL) {
    args.push(
      '--extractor-args',
      `youtubepot-bgutilhttp:base_url=${process.env.POT_PROVIDER_URL}`
    );
  }

  return args;
}

// ================================================================
// COMMON ARGUMENTS
// ================================================================

function addCommonArgs(args) {
  args = addCookieArgs(args);
  args = addProxyArgs(args);
  args = addRemoteComponentArgs(args);
  args = addImpersonateArgs(args);
  args = addPotProviderArgs(args);

  return args;
}

// ================================================================
// RUN YT-DLP
// ================================================================

function runYtDlp(args) {
  return new Promise(
    (resolve, reject) => {
      const finalArgs =
        addCommonArgs([
          ...args,
        ]);

      const proc = spawn(
        YTDLP_PATH,
        finalArgs
      );

      let stdout = '';
      let stderr = '';

      proc.stdout.on(
        'data',
        (chunk) => {
          stdout +=
            chunk.toString();
        }
      );

      proc.stderr.on(
        'data',
        (chunk) => {
          stderr +=
            chunk.toString();
        }
      );

      proc.on(
        'error',
        (err) => {
          reject(
            new Error(
              `Failed to start yt-dlp: ${err.message}`
            )
          );
        }
      );

      proc.on(
        'close',
        (code) => {
          if (code !== 0) {
            reject(
              new Error(
                stderr ||
                  `yt-dlp exited with code ${code}`
              )
            );
          } else {
            if (stderr.trim()) {
              console.log(
                'yt-dlp warnings:',
                stderr.trim()
              );
            }

            resolve(stdout);
          }
        }
      );
    }
  );
}

// ================================================================
// VIDEO INFO
// ================================================================

async function getVideoInfo(url) {
  const output =
    await runYtDlp([
      '--dump-json',
      '--no-playlist',

      /*
       * Preserve approximate manifest
       * filesize values where available.
       */
      '--compat-options',
      'manifest-filesize-approx',

      url,
    ]);

  const data =
    JSON.parse(output);

  const formats =
    (data.formats || [])
      .filter(
        (f) =>
          f.vcodec &&
          f.vcodec !== 'none'
      )
      .map((f) => {
        const exactSize =
          Number.isFinite(
            f.filesize
          )
            ? f.filesize
            : null;

        const approximateSize =
          Number.isFinite(
            f.filesize_approx
          )
            ? f.filesize_approx
            : null;

        const displaySize =
          exactSize ??
          approximateSize ??
          null;

        return {
          format_id:
            f.format_id,

          quality:
            f.format_note ||
            (f.height
              ? `${f.height}p`
              : 'unknown'),

          ext: f.ext,

          height:
            f.height || 0,

          // Exact size.
          filesize:
            exactSize,

          // Estimated size.
          filesizeApprox:
            approximateSize,

          // Frontend uses this.
          displaySize,

          // True when only approximate
          // size is available.
          sizeIsEstimate:
            exactSize === null &&
            approximateSize !== null,

          hasAudio:
            f.acodec !== 'none' &&
            f.acodec !== undefined &&
            f.acodec !== null,
        };
      })
      .filter(
        (f, index, arr) =>
          arr.findIndex(
            (x) =>
              x.quality ===
              f.quality
          ) === index
      )
      .sort(
        (a, b) =>
          b.height -
          a.height
      );

  return {
    title: data.title,
    thumbnail:
      data.thumbnail,
    duration:
      data.duration,
    isLive:
      data.is_live || false,
    uploader:
      data.uploader,
    formats,
  };
}

// ================================================================
// DOWNLOAD
// ================================================================

function runYtDlpDownload({
  url,
  formatSelector,
  outputPath,
  merge,
  onProgress,
  processRef,
}) {
  return new Promise(
    (resolve, reject) => {
      let args = [
        '--no-playlist',

        '-f',
        formatSelector,

        /*
         * Keep partial downloads.
         */
        '--continue',

        /*
         * Do not overwrite an existing
         * completed file.
         */
        '--no-overwrites',

        '--newline',

        '-o',
        outputPath,

        url,
      ];

      if (merge) {
        args.push(
          '--merge-output-format',
          'mp4'
        );
      }

      if (process.env.FFMPEG_PATH) {
        args.push(
          '--ffmpeg-location',
          process.env.FFMPEG_PATH
        );
      }

      args =
        addCommonArgs(args);

      const proc =
        spawn(
          YTDLP_PATH,
          args
        );

      if (processRef) {
        processRef.current =
          proc;
      }

      let stderr = '';
      let stdoutBuffer = '';

      const progressPattern =
        /\[download\]\s+(\d+(?:\.\d+)?)%/;

      proc.stdout.on(
        'data',
        (chunk) => {
          stdoutBuffer +=
            chunk.toString();

          const lines =
            stdoutBuffer.split(
              '\n'
            );

          stdoutBuffer =
            lines.pop();

          if (onProgress) {
            for (
              const line of lines
            ) {
              const match =
                line.match(
                  progressPattern
                );

              if (match) {
                onProgress(
                  parseFloat(
                    match[1]
                  )
                );
              }
            }
          }
        }
      );

      proc.stderr.on(
        'data',
        (chunk) => {
          stderr +=
            chunk.toString();
        }
      );

      proc.on(
        'error',
        (err) => {
          reject(
            new Error(
              `Failed to start yt-dlp: ${err.message}`
            )
          );
        }
      );

      proc.on(
        'close',
        (code, signal) => {
          if (processRef) {
            processRef.current =
              null;
          }

          if (
            signal === 'SIGKILL'
          ) {
            reject(
              new Error(
                'PROCESS_KILLED'
              )
            );

            return;
          }

          if (code !== 0) {
            reject(
              new Error(
                stderr ||
                  `yt-dlp exited with code ${code}`
              )
            );

            return;
          }

          resolve(
            outputPath
          );
        }
      );
    }
  );
}

// ================================================================
// COMBINED DOWNLOAD
// ================================================================

function downloadCombined({
  url,
  formatId,
  hasAudio,
  outputPath,
  onProgress,
  processRef,
}) {
  const formatSelector =
    hasAudio
      ? formatId
      : `${formatId}+bestaudio/best`;

  return runYtDlpDownload({
    url,
    formatSelector,
    outputPath,
    merge: !hasAudio,
    onProgress,
    processRef,
  });
}

// ================================================================
// SINGLE FORMAT
// ================================================================

function downloadSingleFormat({
  url,
  formatId,
  outputPath,
  onProgress,
  processRef,
}) {
  return runYtDlpDownload({
    url,
    formatSelector:
      formatId,
    outputPath,
    merge: false,
    onProgress,
    processRef,
  });
}

// ================================================================
// BEST AUDIO
// ================================================================

function downloadBestAudio({
  url,
  outputPath,
  onProgress,
  processRef,
}) {
  return runYtDlpDownload({
    url,
    formatSelector:
      'bestaudio',
    outputPath,
    merge: false,
    onProgress,
    processRef,
  });
}

// ================================================================
// EXPORTS
// ================================================================

module.exports = {
  runYtDlp,
  getVideoInfo,
  downloadCombined,
  downloadSingleFormat,
  downloadBestAudio,
};
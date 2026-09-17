const { spawn } = require("child_process");
const fs = require("fs");

const YTDLP_PATH = process.env.YTDLP_PATH || "yt-dlp";

const COOKIES_BROWSER = process.env.COOKIES_BROWSER;
const COOKIES_FILE = process.env.COOKIES_FILE;
const PROXY_URL = process.env.PROXY_URL;

// NOTE:
// We intentionally do NOT pass POT_PROVIDER_URL to yt-dlp while using
// web_embedded. web_embedded does not require a PO token, and the PO-token
// provider can cause yt-dlp/YouTube to fall back to an android_vr download
// path that returns HTTP 403.

let ytDlpCommandLogged = false;

function addCommonArgs(args) {
  const common = [
    "--ignore-config",
    "--remote-components",
    "ejs:github",

    // Force the client we already confirmed works manually.
    "--extractor-args",
    "youtube:player_client=web_embedded",
  ];

  if (COOKIES_BROWSER) {
    common.push(
      "--cookies-from-browser",
      COOKIES_BROWSER
    );
  }

  if (COOKIES_FILE) {
    common.push(
      "--cookies",
      COOKIES_FILE
    );
  }

  if (PROXY_URL) {
    common.push(
      "--proxy",
      PROXY_URL
    );
  }

  return [...common, ...args];
}

function logYtDlpCommand(finalArgs) {
  if (ytDlpCommandLogged) {
    return;
  }

  ytDlpCommandLogged = true;

  console.log("[yt-dlp] Executable:", YTDLP_PATH);

  console.log(
    "[yt-dlp] Arguments:",
    finalArgs
      .map((arg) => {
        const value = String(arg);

        if (
          /\s/.test(value) ||
          value.includes("&") ||
          value.includes("?")
        ) {
          return `"${value.replace(/"/g, '\\"')}"`;
        }

        return value;
      })
      .join(" ")
  );
}

function runYtDlp(...args) {
  return new Promise((resolve, reject) => {
    const finalArgs = addCommonArgs(args);

    logYtDlpCommand(finalArgs);

    const child = spawn(
      YTDLP_PATH,
      finalArgs,
      {
        windowsHide: true,
      }
    );

    let stdout = "";
    let stderr = "";

    child.stdout.on("data", (data) => {
      const text = data.toString();

      stdout += text;

      process.stdout.write(text);
    });

    child.stderr.on("data", (data) => {
      const text = data.toString();

      stderr += text;

      process.stderr.write(text);
    });

    child.on("error", (error) => {
      reject(error);
    });

    child.on("close", (code, signal) => {
      if (code === 0) {
        resolve({
          stdout,
          stderr,
        });

        return;
      }

      const error = new Error(
        stderr.trim() ||
          stdout.trim() ||
          `yt-dlp exited with code ${code}`
      );

      error.code = code;
      error.signal = signal;

      reject(error);
    });
  });
}

function getStreamSize(format, duration) {
  if (
    Number.isFinite(format.filesize) &&
    format.filesize > 0
  ) {
    return {
      size: format.filesize,
      estimated: false,
    };
  }

  if (
    Number.isFinite(format.filesize_approx) &&
    format.filesize_approx > 0
  ) {
    return {
      size: format.filesize_approx,
      estimated: true,
    };
  }

  if (duration > 0) {
    let bitrate = 0;

    if (Number.isFinite(format.tbr)) {
      bitrate = format.tbr;
    } else {
      const videoBitrate = Number.isFinite(format.vbr)
        ? format.vbr
        : 0;

      const audioBitrate = Number.isFinite(format.abr)
        ? format.abr
        : 0;

      bitrate = videoBitrate + audioBitrate;
    }

    if (bitrate > 0) {
      return {
        size: (bitrate * 1000 * duration) / 8,
        estimated: true,
      };
    }
  }

  return {
    size: null,
    estimated: true,
  };
}

function findBestAudioFormat(formats) {
  const audioFormats = formats.filter(
    (format) =>
      format.vcodec === "none" &&
      format.acodec &&
      format.acodec !== "none"
  );

  if (!audioFormats.length) {
    return null;
  }

  const m4aFormats = audioFormats.filter(
    (format) => format.ext === "m4a"
  );

  const candidates = m4aFormats.length
    ? m4aFormats
    : audioFormats;

  return [...candidates].sort((a, b) => {
    const aAbr = Number.isFinite(a.abr)
      ? a.abr
      : Number.isFinite(a.tbr)
      ? a.tbr
      : 0;

    const bAbr = Number.isFinite(b.abr)
      ? b.abr
      : Number.isFinite(b.tbr)
      ? b.tbr
      : 0;

    if (bAbr !== aAbr) {
      return bAbr - aAbr;
    }

    const aSize =
      a.filesize ||
      a.filesize_approx ||
      0;

    const bSize =
      b.filesize ||
      b.filesize_approx ||
      0;

    return bSize - aSize;
  })[0];
}

function calculateDownloadSize(
  format,
  allFormats,
  duration
) {
  const videoInfo = getStreamSize(
    format,
    duration
  );

  let totalSize = videoInfo.size;
  let estimated = videoInfo.estimated;

  const hasAudio =
    format.acodec &&
    format.acodec !== "none";

  if (hasAudio) {
    return {
      size: totalSize,
      estimated,
    };
  }

  const bestAudio =
    findBestAudioFormat(allFormats);

  if (
    !bestAudio ||
    totalSize === null
  ) {
    return {
      size: null,
      estimated: true,
    };
  }

  const audioInfo = getStreamSize(
    bestAudio,
    duration
  );

  if (audioInfo.size === null) {
    return {
      size: null,
      estimated: true,
    };
  }

  totalSize += audioInfo.size;
  estimated =
    estimated ||
    audioInfo.estimated;

  return {
    size: totalSize,
    estimated,
  };
}

async function getVideoInfo(url) {
  const { stdout } = await runYtDlp(
    "--dump-json",
    "--no-playlist",
    url
  );

  const data = JSON.parse(stdout);

  const duration =
    Number(data.duration) || 0;

  const rawFormats =
    Array.isArray(data.formats)
      ? data.formats
      : [];

  const videoFormats =
    rawFormats.filter(
      (format) =>
        format.vcodec &&
        format.vcodec !== "none"
    );

  const mappedFormats =
    videoFormats.map((format) => {
      const hasAudio =
        format.acodec &&
        format.acodec !== "none";

      const sizeInfo =
        calculateDownloadSize(
          format,
          rawFormats,
          duration
        );

      return {
        format_id:
          format.format_id,

        quality:
          format.format_note ||
          (
            format.height
              ? `${format.height}p`
              : "unknown"
          ),

        ext:
          format.ext,

        height:
          format.height ||
          null,

        filesize:
          sizeInfo.size,

        filesizeIsEstimate:
          sizeInfo.estimated,

        hasAudio,

        audioCodec:
          format.acodec ||
          null,

        videoCodec:
          format.vcodec ||
          null,

        videoBitrate:
          Number.isFinite(format.vbr)
            ? format.vbr
            : null,

        audioBitrate:
          Number.isFinite(format.abr)
            ? format.abr
            : null,

        totalBitrate:
          Number.isFinite(format.tbr)
            ? format.tbr
            : null,
      };
    });

  const grouped = new Map();

  for (const format of mappedFormats) {
    const key =
      format.height ||
      format.quality;

    if (!grouped.has(key)) {
      grouped.set(key, []);
    }

    grouped
      .get(key)
      .push(format);
  }

  const formats =
    [...grouped.values()]
      .map((group) =>
        [...group].sort(
          (a, b) => {
            const audioScore =
              Number(b.hasAudio) -
              Number(a.hasAudio);

            if (audioScore !== 0) {
              return audioScore;
            }

            const mp4Score =
              Number(b.ext === "mp4") -
              Number(a.ext === "mp4");

            if (mp4Score !== 0) {
              return mp4Score;
            }

            return (
              (b.videoBitrate || 0) -
              (a.videoBitrate || 0)
            );
          }
        )[0]
      )
      .sort(
        (a, b) =>
          (b.height || 0) -
          (a.height || 0)
      );

  return {
    title:
      data.title ||
      "Untitled video",

    thumbnail:
      data.thumbnail ||
      null,

    duration,

    isLive:
      Boolean(data.is_live) ||
      Boolean(
        data.live_status ===
          "is_live"
      ),

    uploader:
      data.uploader ||
      data.channel ||
      null,

    formats,
  };
}

function parseSizeToBytes(value) {
  if (!value) {
    return null;
  }

  const match = String(value)
    .trim()
    .match(
      /^([\d.]+)\s*(KiB|MiB|GiB|TiB|KB|MB|GB|TB|B)$/i
    );

  if (!match) {
    return null;
  }

  const amount = Number(match[1]);
  const unit =
    match[2].toLowerCase();

  const multipliers = {
    b: 1,
    kib: 1024,
    mib: 1024 ** 2,
    gib: 1024 ** 3,
    tib: 1024 ** 4,
    kb: 1000,
    mb: 1000 ** 2,
    gb: 1000 ** 3,
    tb: 1000 ** 4,
  };

  const multiplier =
    multipliers[unit];

  if (!multiplier) {
    return null;
  }

  return amount * multiplier;
}

function parseDownloadProgress(line) {
  if (!line.includes("[download]")) {
    return null;
  }

  const percentMatch =
    line.match(
      /\[download\]\s+(\d+(?:\.\d+)?)%/
    );

  if (!percentMatch) {
    return null;
  }

  const percent =
    Number(percentMatch[1]);

  let downloadedBytes = null;
  let totalBytes = null;

  const totalMatch =
    line.match(
      /\bof\s+([\d.]+\s*(?:KiB|MiB|GiB|TiB|KB|MB|GB|TB|B))/i
    );

  if (totalMatch) {
    totalBytes =
      parseSizeToBytes(
        totalMatch[1]
      );
  }

  if (
    totalBytes !== null &&
    Number.isFinite(percent)
  ) {
    downloadedBytes =
      totalBytes *
      (percent / 100);
  }

  let speed = null;

  const speedMatch =
    line.match(
      /\bat\s+([\d.]+\s*(?:KiB|MiB|GiB|TiB|KB|MB|GB|TB)\/s)/i
    );

  if (speedMatch) {
    speed =
      parseSizeToBytes(
        speedMatch[1].replace(
          /\/s$/i,
          ""
        )
      );
  }

  return {
    percent,
    downloadedBytes,
    totalBytes,
    speed,
  };
}

function runYtDlpDownload({
  url,
  formatSelector,
  outputPath,
  merge = false,
  ffmpegPath,
  onProgress,
  onProgressDetails,
}) {
  return new Promise(
    (resolve, reject) => {
      const args = [
        "--no-playlist",
        "-f",
        formatSelector,
        "--newline",
        "-o",
        outputPath,
        url,
      ];

      if (merge) {
        args.push(
          "--merge-output-format",
          "mp4"
        );
      }

      if (ffmpegPath) {
        args.push(
          "--ffmpeg-location",
          ffmpegPath
        );
      }

      const finalArgs =
        addCommonArgs(args);

      logYtDlpCommand(
        finalArgs
      );

      const child = spawn(
        YTDLP_PATH,
        finalArgs,
        {
          windowsHide: true,
        }
      );

      let stderr = "";
      let stdoutBuffer = "";

      const streams = new Map();

      let currentStream =
        "default";

      let lastProgress = {
        percent: 0,
        downloadedBytes: null,
        totalBytes: null,
        speed: null,
      };

      child.stdout.on(
        "data",
        (data) => {
          const text =
            data.toString();

          process.stdout.write(
            text
          );

          stdoutBuffer += text;

          const lines =
            stdoutBuffer.split(
              /\r?\n/
            );

          stdoutBuffer =
            lines.pop() || "";

          for (
            const line of lines
          ) {
            const destinationMatch =
              line.match(
                /\[download\]\s+Destination:\s+(.+)/
              );

            if (
              destinationMatch
            ) {
              currentStream =
                destinationMatch[1].trim();

              if (
                !streams.has(
                  currentStream
                )
              ) {
                streams.set(
                  currentStream,
                  {
                    downloadedBytes: 0,
                    totalBytes: null,
                    speed: null,
                  }
                );
              }

              continue;
            }

            const progress =
              parseDownloadProgress(
                line
              );

            if (!progress) {
              continue;
            }

            const stream =
              streams.get(
                currentStream
              ) || {
                downloadedBytes: 0,
                totalBytes: null,
                speed: null,
              };

            if (
              Number.isFinite(
                progress.downloadedBytes
              )
            ) {
              stream.downloadedBytes =
                progress.downloadedBytes;
            }

            if (
              Number.isFinite(
                progress.totalBytes
              ) &&
              progress.totalBytes > 0
            ) {
              stream.totalBytes =
                progress.totalBytes;
            }

            if (
              Number.isFinite(
                progress.speed
              )
            ) {
              stream.speed =
                progress.speed;
            }

            streams.set(
              currentStream,
              stream
            );

            let downloadedBytes = 0;
            let totalBytes = 0;
            let hasTotal = false;
            let speed = null;

            for (
              const item of
              streams.values()
            ) {
              if (
                Number.isFinite(
                  item.downloadedBytes
                )
              ) {
                downloadedBytes +=
                  item.downloadedBytes;
              }

              if (
                Number.isFinite(
                  item.totalBytes
                ) &&
                item.totalBytes > 0
              ) {
                totalBytes +=
                  item.totalBytes;

                hasTotal = true;
              }

              if (
                Number.isFinite(
                  item.speed
                )
              ) {
                speed =
                  (speed || 0) +
                  item.speed;
              }
            }

            const aggregatePercent =
              hasTotal &&
              totalBytes > 0
                ? Math.min(
                    100,
                    (
                      downloadedBytes /
                      totalBytes
                    ) * 100
                  )
                : progress.percent;

            lastProgress = {
              percent:
                aggregatePercent,

              downloadedBytes,

              totalBytes:
                hasTotal
                  ? totalBytes
                  : null,

              speed,
            };

            if (
              typeof onProgress ===
              "function"
            ) {
              onProgress(
                aggregatePercent
              );
            }

            if (
              typeof onProgressDetails ===
              "function"
            ) {
              onProgressDetails(
                lastProgress
              );
            }
          }
        }
      );

      child.stderr.on(
        "data",
        (data) => {
          stderr +=
            data.toString();
        }
      );

      child.on(
        "error",
        reject
      );

      child.on(
        "close",
        (code, signal) => {
          if (code === 0) {
            if (
              lastProgress.totalBytes &&
              lastProgress.totalBytes > 0
            ) {
              lastProgress = {
                ...lastProgress,
                percent: 100,
                downloadedBytes:
                  lastProgress.totalBytes,
              };
            } else {
              lastProgress = {
                ...lastProgress,
                percent: 100,
              };
            }

            if (
              typeof onProgress ===
              "function"
            ) {
              onProgress(100);
            }

            if (
              typeof onProgressDetails ===
              "function"
            ) {
              onProgressDetails(
                lastProgress
              );
            }

            resolve();
            return;
          }

          const error =
            new Error(
              stderr.trim() ||
                `yt-dlp exited with code ${code}`
            );

          error.code = code;
          error.signal = signal;

          if (
            signal === "SIGKILL"
          ) {
            error.code =
              "PROCESS_KILLED";
          }

          reject(error);
        }
      );
    }
  );
}

async function downloadCombined({
  url,
  formatId,
  height,
  hasAudio,
  outputPath,
  ffmpegPath,
  onProgress,
  onProgressDetails,
}) {
  if (hasAudio) {
    return runYtDlpDownload({
      url,
      formatSelector: formatId,
      outputPath,
      merge: false,
      ffmpegPath,
      onProgress,
      onProgressDetails,
    });
  }

  const selectedHeight =
    Number(height);

  const selectors = [];

  if (
    Number.isFinite(
      selectedHeight
    ) &&
    selectedHeight > 0
  ) {
    selectors.push(
      `${formatId}+bestaudio[ext=m4a]`,
      `${formatId}+bestaudio`,
      `best[height=${selectedHeight}][ext=mp4]`,
      `best[height=${selectedHeight}]`,
      `bestvideo[height=${selectedHeight}][ext=mp4]+bestaudio[ext=m4a]`,
      `bestvideo[height=${selectedHeight}]+bestaudio`
    );
  } else {
    selectors.push(
      `${formatId}+bestaudio[ext=m4a]`,
      `${formatId}+bestaudio`,
      `${formatId}`
    );
  }

  let lastError = null;

  for (
    const selector of selectors
  ) {
    try {
      return await runYtDlpDownload({
        url,
        formatSelector: selector,
        outputPath,
        merge: true,
        ffmpegPath,
        onProgress,
        onProgressDetails,
      });
    } catch (error) {
      lastError = error;

      const message =
        String(
          error?.message || ""
        );

      if (
        !/403|forbidden/i.test(
          message
        )
      ) {
        throw error;
      }

      try {
        fs.unlinkSync(
          outputPath
        );
      } catch {
        // Ignore missing output file.
      }
    }
  }

  throw (
    lastError ||
    new Error(
      "Unable to download the selected video format"
    )
  );
}

async function downloadSingleFormat({
  url,
  formatId,
  outputPath,
  ffmpegPath,
  onProgress,
  onProgressDetails,
}) {
  return runYtDlpDownload({
    url,
    formatSelector: formatId,
    outputPath,
    merge: false,
    ffmpegPath,
    onProgress,
    onProgressDetails,
  });
}

async function downloadBestAudio({
  url,
  outputPath,
  ffmpegPath,
  onProgress,
  onProgressDetails,
}) {
  return runYtDlpDownload({
    url,
    formatSelector:
      "bestaudio[ext=m4a]/bestaudio",
    outputPath,
    merge: false,
    ffmpegPath,
    onProgress,
    onProgressDetails,
  });
}

module.exports = {
  runYtDlp,
  getVideoInfo,
  downloadCombined,
  downloadSingleFormat,
  downloadBestAudio,
};
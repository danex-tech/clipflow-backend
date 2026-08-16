const { spawn } = require('child_process');

// Path to the yt-dlp executable.
// On Windows, if yt-dlp isn't recognized in your terminal globally,
// set YTDLP_PATH in your .env file to the full path, e.g.:
// YTDLP_PATH=C:\Users\Daniel\AppData\Local\Python\pythoncore-3.14-64\Scripts\yt-dlp.exe
const YTDLP_PATH = process.env.YTDLP_PATH || 'yt-dlp';

// Optional: name of a browser to borrow cookies from (e.g. "chrome", "edge", "firefox").
// Less reliable on Windows since Chromium browsers often lock their cookie file.
const COOKIES_BROWSER = process.env.YTDLP_COOKIES_BROWSER;

// Optional: path to an exported cookies.txt file (more reliable than COOKIES_BROWSER).
// If set, this takes priority.
const COOKIES_FILE = process.env.YTDLP_COOKIES_FILE;

function addCookieArgs(args) {
  if (COOKIES_FILE) {
    args.push('--cookies', COOKIES_FILE);
  } else if (COOKIES_BROWSER) {
    args.push('--cookies-from-browser', COOKIES_BROWSER);
  }
  return args;
}

// Optional: route yt-dlp's traffic through a proxy (Cloudflare WARP's local
// SOCKS5 proxy at socks5://127.0.0.1:40000), set by start.sh if WARP
// connects successfully on Render. Locally this stays unset.
function addProxyArgs(args) {
  if (process.env.PROXY_URL) {
    args.push('--proxy', process.env.PROXY_URL);
  }
  return args;
}

// Newer yt-dlp versions require explicit permission to download/use their
// updated JS-challenge-solving component — without this, it silently skips
// solving YouTube's challenges and most formats become unavailable, even
// with Deno already installed.
function addRemoteComponentArgs(args) {
  args.push('--remote-components', 'ejs:github');
  return args;
}

// Some platforms (TikTok especially) reject requests unless they look like
// they're coming from a real browser at the network level, not just a
// normal User-Agent header. --impersonate does this properly using
// curl_cffi. Harmless for platforms that don't need it.
function addImpersonateArgs(args) {
  args.push('--impersonate', 'chrome');
  return args;
}

// Optional: points yt-dlp at a PO Token provider server, needed to unlock
// YouTube's higher-quality formats. Runs as its own separate service (see
// POT_PROVIDER_URL in .env) rather than locally, to keep this container's
// memory usage low. If unset, YouTube quality caps around 360p — everything
// else is unaffected either way.
function addPotProviderArgs(args) {
  if (process.env.POT_PROVIDER_URL) {
    args.push('--extractor-args', `youtubepot-bgutilhttp:base_url=${process.env.POT_PROVIDER_URL}`);
  }
  return args;
}

function addCommonArgs(args) {
  args = addCookieArgs(args);
  args = addProxyArgs(args);
  args = addRemoteComponentArgs(args);
  args = addImpersonateArgs(args);
  args = addPotProviderArgs(args);
  return args;
}

/**
 * Runs yt-dlp with the given arguments and returns stdout as a string.
 * Rejects with a readable error message if yt-dlp fails.
 */
function runYtDlp(args) {
  return new Promise((resolve, reject) => {
    const proc = spawn(YTDLP_PATH, addCookieArgs(args));

    let stdout = '';
    let stderr = '';

    proc.stdout.on('data', (chunk) => {
      stdout += chunk.toString();
    });

    proc.stderr.on('data', (chunk) => {
      stderr += chunk.toString();
    });

    proc.on('error', (err) => {
      reject(new Error(`Failed to start yt-dlp: ${err.message}`));
    });

    proc.on('close', (code) => {
      if (code !== 0) {
        reject(new Error(stderr || `yt-dlp exited with code ${code}`));
      } else {
        if (stderr.trim()) {
          console.log('yt-dlp warnings:', stderr.trim());
        }
        resolve(stdout);
      }
    });
  });
}

/**
 * Fetches metadata + available formats for a given video URL.
 * Each format now also reports hasAudio, so the download step knows
 * whether it's a single combined stream or needs a separate audio track.
 */
async function getVideoInfo(url) {
  const output = await runYtDlp(addPotProviderArgs(addImpersonateArgs(addRemoteComponentArgs(addProxyArgs(['--dump-json', '--no-playlist', url])))));
  const data = JSON.parse(output);

  const formats = (data.formats || [])
    .filter((f) => f.vcodec !== 'none')
    .map((f) => ({
      format_id: f.format_id,
      quality: f.format_note || (f.height ? `${f.height}p` : 'unknown'),
      ext: f.ext,
      height: f.height || 0,
      filesize: f.filesize || f.filesize_approx || null,
      hasAudio: f.acodec !== 'none' && f.acodec !== undefined && f.acodec !== null,
    }))
    .filter((f, index, arr) => arr.findIndex((x) => x.quality === f.quality) === index)
    .sort((a, b) => b.height - a.height);

  return {
    title: data.title,
    thumbnail: data.thumbnail,
    duration: data.duration,
    isLive: data.is_live || false,
    uploader: data.uploader,
    formats,
  };
}

/**
 * Runs a yt-dlp download with a given format selector, reporting live
 * progress and exposing the running process via processRef so the caller
 * can kill it (used for cancellation). Shared by every download variant
 * below so there's only one place that actually spawns yt-dlp for downloads.
 */
function runYtDlpDownload({ url, formatSelector, outputPath, merge, onProgress, processRef }) {
  return new Promise((resolve, reject) => {
    let args = ['--no-playlist', '-f', formatSelector, '--newline', '-o', outputPath, url];

    if (merge) {
      args.push('--merge-output-format', 'mp4');
    }

    if (process.env.FFMPEG_PATH) {
      args.push('--ffmpeg-location', process.env.FFMPEG_PATH);
    }

    args = addCommonArgs(args);

    const proc = spawn(YTDLP_PATH, args);
    if (processRef) processRef.current = proc;

    let stderr = '';
    let stdoutBuffer = '';
    // yt-dlp prints lines like: [download]  45.2% of   10.00MiB at  1.20MiB/s ETA 00:04
    const progressPattern = /\[download\]\s+(\d+(?:\.\d+)?)%/;

    proc.stdout.on('data', (chunk) => {
      stdoutBuffer += chunk.toString();
      const lines = stdoutBuffer.split('\n');
      stdoutBuffer = lines.pop();

      if (onProgress) {
        for (const line of lines) {
          const match = line.match(progressPattern);
          if (match) {
            onProgress(parseFloat(match[1]));
          }
        }
      }
    });

    proc.stderr.on('data', (chunk) => {
      stderr += chunk.toString();
    });

    proc.on('error', (err) => {
      reject(new Error(`Failed to start yt-dlp: ${err.message}`));
    });

    proc.on('close', (code, signal) => {
      if (processRef) processRef.current = null;
      if (signal === 'SIGKILL') {
        // Process was deliberately killed (cancellation) — let the caller's
        // own cancellation check handle this, not a generic failure.
        reject(new Error('PROCESS_KILLED'));
      } else if (code !== 0) {
        reject(new Error(stderr || `yt-dlp exited with code ${code}`));
      } else {
        resolve(outputPath);
      }
    });
  });
}

/**
 * Downloads a video for the "no trim" path. If the format already has
 * audio, downloads it alone (fast). Otherwise asks yt-dlp to merge in the
 * best available audio track itself.
 */
function downloadCombined({ url, formatId, hasAudio, outputPath, onProgress, processRef }) {
  const formatSelector = hasAudio ? formatId : `${formatId}+bestaudio/best`;
  return runYtDlpDownload({ url, formatSelector, outputPath, merge: !hasAudio, onProgress, processRef });
}

/**
 * Downloads exactly one raw stream (no merging) — used by the trim
 * pipeline for both combined-format and video-only downloads.
 */
function downloadSingleFormat({ url, formatId, outputPath, onProgress, processRef }) {
  return runYtDlpDownload({ url, formatSelector: formatId, outputPath, merge: false, onProgress, processRef });
}

/**
 * Downloads just the best available audio track alone — used by the trim
 * pipeline when the chosen video format has no audio of its own.
 */
function downloadBestAudio({ url, outputPath, onProgress, processRef }) {
  return runYtDlpDownload({ url, formatSelector: 'bestaudio', outputPath, merge: false, onProgress, processRef });
}

module.exports = {
  runYtDlp,
  getVideoInfo,
  downloadCombined,
  downloadSingleFormat,
  downloadBestAudio,
};
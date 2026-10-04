# Start from an official Node.js image (Debian-based, so apt-get works)
FROM node:20-bookworm-slim

# Install system-level tools our app depends on:
# - python3 + pip: needed to run yt-dlp
# - ffmpeg: needed for merging/trimming video
# - curl + unzip: needed to install Deno
# - gnupg: needed to set up Cloudflare WARP's package repository
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    ffmpeg \
    curl \
    unzip \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# Install the latest yt-dlp and its recommended default dependencies.
# Print the installed version during the Docker build so it can be verified
# directly in the Render build logs.
RUN python3 -m pip install --break-system-packages --no-cache-dir -U "yt-dlp[default]" \
    && python3 -m yt_dlp --version

# Install curl_cffi, needed for --impersonate (used on every request now,
# not just TikTok).
RUN python3 -m pip install --break-system-packages --no-cache-dir "curl_cffi==0.13.0"

# Install the lightweight Python-side PO Token plugin.
# The actual token-generating server runs as a separate Render service.
RUN python3 -m pip install --break-system-packages --no-cache-dir -U bgutil-ytdlp-pot-provider

# Install Deno (needed by yt-dlp for JavaScript challenge solving)
RUN curl -fsSL https://deno.land/install.sh | sh
ENV DENO_INSTALL="/root/.deno"
ENV PATH="$DENO_INSTALL/bin:$PATH"

# Install Cloudflare WARP client
RUN curl -fsSL https://pkg.cloudflareclient.com/pubkey.gpg | gpg --dearmor -o /usr/share/keyrings/cloudflare-warp-archive-keyring.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/cloudflare-warp-archive-keyring.gpg] https://pkg.cloudflareclient.com/ bookworm main" > /etc/apt/sources.list.d/cloudflare-client.list \
    && apt-get update \
    && apt-get install -y cloudflare-warp \
    && rm -rf /var/lib/apt/lists/*

# Set up the app directory
WORKDIR /app

# Install Node dependencies first for better Docker layer caching
COPY package*.json ./
RUN npm install --omit=dev

# Copy the rest of the application
COPY . .

# Render sets $PORT automatically
EXPOSE 5000

RUN chmod +x start.sh

# Attempts to connect Cloudflare WARP, then launches the API and worker
CMD ["./start.sh"]
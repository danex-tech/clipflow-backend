#!/bin/sh

# Copy Render's read-only secret cookies file to a writable location.
if [ -f /etc/secrets/cookies.txt ]; then
  cp /etc/secrets/cookies.txt /tmp/cookies.txt
fi

echo "Attempting to start Cloudflare WARP..."

# Start WARP daemon
warp-svc &
sleep 5

# Register WARP if this container does not already have a registration.
warp-cli --accept-tos registration new 2>&1 || true

# Use WARP local proxy mode.
warp-cli --accept-tos mode proxy 2>&1

# Connect WARP.
warp-cli --accept-tos connect 2>&1
sleep 5

echo "Checking WARP status..."
warp-cli --accept-tos status 2>&1

# Tell yt-dlp to actually use WARP's SOCKS5 proxy.
export PROXY_URL="socks5://127.0.0.1:40000"

echo "WARP proxy configured: $PROXY_URL"

# Start the application.
npm start
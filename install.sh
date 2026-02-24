#!/bin/sh
set -e

REPO="ankitpatial/zqlc"
INSTALL_DIR="/usr/local/bin"

# Detect OS
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
case "$OS" in
  linux)  OS="linux" ;;
  darwin) OS="macos" ;;
  *)      echo "Unsupported OS: $OS"; exit 1 ;;
esac

# Detect architecture
ARCH=$(uname -m)
case "$ARCH" in
  x86_64|amd64)  ARCH="x86_64" ;;
  aarch64|arm64)  ARCH="aarch64" ;;
  *)              echo "Unsupported architecture: $ARCH"; exit 1 ;;
esac

# Fetch latest release tag
echo "Fetching latest release..."
TAG=$(curl -sfL "https://api.github.com/repos/${REPO}/releases/latest" | grep '"tag_name"' | sed 's/.*"tag_name": *"\([^"]*\)".*/\1/')

if [ -z "$TAG" ]; then
  echo "Failed to fetch latest release. Check your internet connection."
  exit 1
fi

echo "Latest version: $TAG"

# Download
URL="https://github.com/${REPO}/releases/download/${TAG}/zqlc-${OS}-${ARCH}.tar.gz"
echo "Downloading ${URL}..."
curl -sfL -o /tmp/zqlc.tar.gz "$URL"

# Extract
tar -xzf /tmp/zqlc.tar.gz -C /tmp zqlc

# Install
if [ -w "$INSTALL_DIR" ]; then
  mv /tmp/zqlc "$INSTALL_DIR/zqlc"
else
  echo "Installing to ${INSTALL_DIR} (requires sudo)..."
  sudo mv /tmp/zqlc "$INSTALL_DIR/zqlc"
fi

chmod +x "$INSTALL_DIR/zqlc"

# Clean up
rm -f /tmp/zqlc.tar.gz

echo "zqlc ${TAG} installed to ${INSTALL_DIR}/zqlc"
echo "Run 'zqlc --help' to get started."

#!/bin/sh
# xdu installer script
# Usage: curl -sSfL https://raw.githubusercontent.com/glentner/xdu/main/install.sh | sh
#
# Environment variables:
#   XDU_VERSION  - Version to install (default: latest)
#   XDU_INSTALL  - Installation directory (default: ~/.local/bin)

set -eu

REPO="glentner/xdu"
BINARY_NAME="xdu"

# Colors for output (disabled if not a terminal)
if [ -t 1 ]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[0;33m'
    BLUE='\033[0;34m'
    NC='\033[0m' # No Color
else
    RED=''
    GREEN=''
    YELLOW=''
    BLUE=''
    NC=''
fi

info() {
    printf "${BLUE}info${NC}: %s\n" "$1"
}

warn() {
    printf "${YELLOW}warn${NC}: %s\n" "$1" >&2
}

error() {
    printf "${RED}error${NC}: %s\n" "$1" >&2
    exit 1
}

success() {
    printf "${GREEN}success${NC}: %s\n" "$1"
}

# Detect OS
detect_os() {
    case "$(uname -s)" in
        Linux*)  echo "linux" ;;
        Darwin*) echo "macos" ;;
        *)       error "Unsupported operating system: $(uname -s)" ;;
    esac
}

# Detect architecture
detect_arch() {
    case "$(uname -m)" in
        x86_64|amd64)  echo "x86_64" ;;
        aarch64|arm64) echo "aarch64" ;;
        *)             error "Unsupported architecture: $(uname -m)" ;;
    esac
}

# Get the latest release version from GitHub
get_latest_version() {
    if command -v curl >/dev/null 2>&1; then
        curl -sSfL "https://api.github.com/repos/${REPO}/releases/latest" | 
            grep '"tag_name":' | 
            sed -E 's/.*"([^"]+)".*/\1/'
    elif command -v wget >/dev/null 2>&1; then
        wget -qO- "https://api.github.com/repos/${REPO}/releases/latest" | 
            grep '"tag_name":' | 
            sed -E 's/.*"([^"]+)".*/\1/'
    else
        error "Neither curl nor wget found. Please install one of them."
    fi
}

# Download a file
download() {
    url="$1"
    output="$2"
    
    if command -v curl >/dev/null 2>&1; then
        curl -sSfL "$url" -o "$output"
    elif command -v wget >/dev/null 2>&1; then
        wget -q "$url" -O "$output"
    else
        error "Neither curl nor wget found. Please install one of them."
    fi
}

# Main installation function
main() {
    OS=$(detect_os)
    ARCH=$(detect_arch)
    
    info "Detected OS: ${OS}, Arch: ${ARCH}"
    
    # Determine version to install
    VERSION="${XDU_VERSION:-}"
    if [ -z "$VERSION" ]; then
        info "Fetching latest release..."
        VERSION=$(get_latest_version)
        if [ -z "$VERSION" ]; then
            error "Could not determine latest version. Set XDU_VERSION to install a specific version."
        fi
    fi
    
    info "Installing xdu ${VERSION}"
    
    # Determine installation directory
    INSTALL_DIR="${XDU_INSTALL:-$HOME/.local/bin}"
    
    # Create installation directory if it doesn't exist
    if [ ! -d "$INSTALL_DIR" ]; then
        info "Creating installation directory: ${INSTALL_DIR}"
        mkdir -p "$INSTALL_DIR"
    fi
    
    # Construct target triple
    case "${OS}-${ARCH}" in
        linux-x86_64)   TARGET="x86_64-unknown-linux-gnu" ;;
        linux-aarch64)  TARGET="aarch64-unknown-linux-gnu" ;;
        macos-x86_64)   TARGET="x86_64-apple-darwin" ;;
        macos-aarch64)  TARGET="aarch64-apple-darwin" ;;
        *)              error "Unsupported platform: ${OS}-${ARCH}" ;;
    esac
    
    # Construct download URL
    # Expected asset name format: xdu-{version}-{target}.tar.gz
    ARCHIVE_NAME="${BINARY_NAME}-${VERSION}-${TARGET}.tar.gz"
    DOWNLOAD_URL="https://github.com/${REPO}/releases/download/${VERSION}/${ARCHIVE_NAME}"
    
    # Create temporary directory
    TMPDIR=$(mktemp -d)
    trap 'rm -rf "$TMPDIR"' EXIT
    
    ARCHIVE_PATH="${TMPDIR}/${ARCHIVE_NAME}"
    
    info "Downloading ${DOWNLOAD_URL}"
    if ! download "$DOWNLOAD_URL" "$ARCHIVE_PATH"; then
        error "Failed to download ${DOWNLOAD_URL}"
    fi
    
    info "Extracting to ${INSTALL_DIR}"
    tar -xzf "$ARCHIVE_PATH" -C "$TMPDIR"
    
    # Install binaries
    # The archive should contain: xdu, xdu-find, xdu-view
    for bin in xdu xdu-find xdu-view; do
        if [ -f "${TMPDIR}/${bin}" ]; then
            mv "${TMPDIR}/${bin}" "${INSTALL_DIR}/${bin}"
            chmod +x "${INSTALL_DIR}/${bin}"
        fi
    done
    
    success "Installed xdu ${VERSION} to ${INSTALL_DIR}"
    
    # Check if install directory is in PATH
    case ":$PATH:" in
        *":${INSTALL_DIR}:"*) ;;
        *)
            warn "${INSTALL_DIR} is not in your PATH"
            echo ""
            echo "Add this to your shell configuration file (~/.bashrc, ~/.zshrc, etc.):"
            echo ""
            echo "    export PATH=\"\$HOME/.local/bin:\$PATH\""
            echo ""
            ;;
    esac
    
    echo ""
    echo "Run 'xdu --help' to get started"
}

main "$@"

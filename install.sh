#!/bin/sh
# xdu installer script
# Usage: curl -sSfL https://raw.githubusercontent.com/glentner/xdu/main/install.sh | sh
#
# Environment variables:
#   XDU_VERSION  - Version to install (default: latest)
#   XDU_INSTALL  - Installation directory for binaries (default: ~/.local/bin)
#   XDU_PREFIX   - Prefix for share/ files: man pages, completions (default: ~/.local)

set -eu

REPO="glentner/xdu"
BINARIES="xdu xdu-find xdu-view xdu-rm"

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

    # Determine installation directories
    PREFIX="${XDU_PREFIX:-$HOME/.local}"
    INSTALL_DIR="${XDU_INSTALL:-${PREFIX}/bin}"

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
    ARCHIVE_NAME="xdu-${VERSION}-${TARGET}.tar.gz"
    DOWNLOAD_URL="https://github.com/${REPO}/releases/download/${VERSION}/${ARCHIVE_NAME}"

    # Create temporary directory
    TMPDIR=$(mktemp -d)
    trap 'rm -rf "$TMPDIR"' EXIT

    ARCHIVE_PATH="${TMPDIR}/${ARCHIVE_NAME}"

    info "Downloading ${DOWNLOAD_URL}"
    if ! download "$DOWNLOAD_URL" "$ARCHIVE_PATH"; then
        error "Failed to download ${DOWNLOAD_URL}"
    fi

    info "Extracting archive"
    tar -xzf "$ARCHIVE_PATH" -C "$TMPDIR"

    # Install binaries from bin/
    for bin in $BINARIES; do
        if [ -f "${TMPDIR}/bin/${bin}" ]; then
            mv "${TMPDIR}/bin/${bin}" "${INSTALL_DIR}/${bin}"
            chmod +x "${INSTALL_DIR}/${bin}"
        fi
    done
    success "Installed binaries to ${INSTALL_DIR}"

    # Install man pages
    MAN_DIR="${PREFIX}/share/man/man1"
    if [ -d "${TMPDIR}/share/man/man1" ]; then
        mkdir -p "$MAN_DIR"
        cp "${TMPDIR}/share/man/man1/"*.1 "$MAN_DIR/"
        success "Installed man pages to ${MAN_DIR}"
    fi

    # Install bash completions
    BASH_COMP_DIR="${PREFIX}/share/bash-completion/completions"
    if [ -d "${TMPDIR}/share/bash-completion/completions" ]; then
        mkdir -p "$BASH_COMP_DIR"
        cp "${TMPDIR}/share/bash-completion/completions/"* "$BASH_COMP_DIR/"
        success "Installed bash completions to ${BASH_COMP_DIR}"
    fi

    # Install zsh completions
    ZSH_COMP_DIR="${PREFIX}/share/zsh/site-functions"
    if [ -d "${TMPDIR}/share/zsh/site-functions" ]; then
        mkdir -p "$ZSH_COMP_DIR"
        cp "${TMPDIR}/share/zsh/site-functions/"* "$ZSH_COMP_DIR/"
        success "Installed zsh completions to ${ZSH_COMP_DIR}"
    fi

    # Check if install directory is in PATH
    case ":$PATH:" in
        *":${INSTALL_DIR}:"*) ;;
        *)
            warn "${INSTALL_DIR} is not in your PATH"
            echo ""
            echo "Add this to your shell configuration file (~/.bashrc, ~/.zshrc, etc.):"
            echo ""
            echo "    export PATH=\"${INSTALL_DIR}:\$PATH\""
            echo ""
            ;;
    esac

    # Shell configuration guidance
    echo ""
    echo "To enable bash completions, ensure bash-completion is loaded and add:"
    echo "    export XDG_DATA_DIRS=\"${PREFIX}/share:\$XDG_DATA_DIRS\""
    echo ""
    echo "To enable zsh completions, add to your ~/.zshrc (before compinit):"
    echo "    fpath=(${ZSH_COMP_DIR} \$fpath)"
    echo ""
    echo "Man pages are available via: man xdu"
    echo ""
    echo "Run 'xdu --help' to get started."
}

main "$@"

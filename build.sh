#!/bin/sh
# Build release binaries for the current platform.
#
# Usage:
#   ./build.sh          # Build for current platform
#   ./build.sh --target x86_64-unknown-linux-musl  # Cross-compile
#
# Output: target/<target>/release/{xdu,xdu-find,xdu-view}

set -eu

# Detect target if not specified
if [ $# -eq 0 ]; then
    case "$(uname -s)-$(uname -m)" in
        Linux-x86_64)   TARGET="x86_64-unknown-linux-musl" ;;
        Linux-aarch64)  TARGET="aarch64-unknown-linux-musl" ;;
        Darwin-x86_64)  TARGET="x86_64-apple-darwin" ;;
        Darwin-arm64)   TARGET="aarch64-apple-darwin" ;;
        *)
            echo "error: Unsupported platform: $(uname -s)-$(uname -m)" >&2
            exit 1
            ;;
    esac
else
    # Allow --target <target> or just <target>
    if [ "$1" = "--target" ]; then
        TARGET="$2"
    else
        TARGET="$1"
    fi
fi

echo "Building for target: $TARGET"

# Ensure the target is installed
rustup target add "$TARGET" 2>/dev/null || true

# Build
cargo build --release --target "$TARGET"

# Report output location
echo ""
echo "Binaries built:"
ls -lh "target/$TARGET/release/xdu" "target/$TARGET/release/xdu-find" "target/$TARGET/release/xdu-view" 2>/dev/null || true

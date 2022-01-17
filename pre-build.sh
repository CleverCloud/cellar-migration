#!/bin/sh

# Script until https://github.com/rust-build/rust-build.action
# gets a new release
echo "Running pre-build script"
sed '/^export CXX.*/a export PATH="/opt/osxcross/target/bin:$PATH"\nexport LIBZ_SYS_STATIC=1' -i /build.sh

echo "Installing needed dependencies"
apk add make perl gcc

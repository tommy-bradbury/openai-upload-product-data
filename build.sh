#!/bin/bash

# Exit if a command exits with a non-zero status.
set -e

GOOS=linux
GOARCH=amd64
CGO_ENABLED=0
BUILD_DIR="bin"
EXECUTABLE_NAME="bootstrap"

mkdir -p "$BUILD_DIR"
echo "Building..."
cd src
go build -o "../$BUILD_DIR/$EXECUTABLE_NAME" -ldflags="-s -w" .
cd ..
echo "DONE! '$EXECUTABLE_NAME' zai '$BUILD_DIR/'."
cd "$BUILD_DIR"
zip "${EXECUTABLE_NAME}.zip" "$EXECUTABLE_NAME"
mv "${EXECUTABLE_NAME}.zip" ../
cd ..
echo "zip '${EXECUTABLE_NAME}.zip' created."
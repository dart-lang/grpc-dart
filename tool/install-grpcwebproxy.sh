#!/bin/sh

set -ex

VERSION=v0.13.0
SUFFIX=
WGET=wget

case $TRAVIS_OS_NAME in
  linux)
    VARIANT=linux-x86_64
    ;;
  osx)
    VARIANT=osx-x86_64
    ;;
  windows)
    VARIANT=win64.exe
    SUFFIX=.exe
    ;;
esac

case $MATRIX_OS in
  ubuntu-latest)
    VARIANT=linux-x86_64
    ;;
  macos-latest)
    VARIANT=osx-x86_64
    ;;
  windows-latest)
    VARIANT=win64.exe
    SUFFIX=.exe
    WGET=C:/msys64/usr/bin/wget.exe
    ;;
esac

BINARY=grpcwebproxy-${VERSION}-${VARIANT}

${WGET} https://github.com/improbable-eng/grpc-web/releases/download/${VERSION}/${BINARY}.zip -O /tmp/grpcwebproxy.zip
rm -rf /tmp/grpcwebproxy
mkdir /tmp/grpcwebproxy
cd /tmp/grpcwebproxy
unzip /tmp/grpcwebproxy.zip
mv dist/${BINARY} ./grpcwebproxy${SUFFIX}


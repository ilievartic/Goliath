#!/bin/sh

if [ $# -gt 0 ]; then
    VERSION=$1
else
    VERSION=0.0.0
fi

VERSION=$VERSION python3 pypi.py bdist_wheel && python3 -m pip install dist/goliath-$VERSION-*.whl
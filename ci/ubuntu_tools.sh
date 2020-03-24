#!/bin/bash

echo "Setup tools for Ubuntu"

# Install CCM
git clone --branch master --single-branch https://github.com/riptano/ccm.git
pushd ccm || exit
sudo python setup.py install
popd
ccm status || true

export CCM_PATH="$(pwd)/ccm"
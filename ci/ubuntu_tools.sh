#!/bin/bash

echo "Setup tools for Ubuntu"

# Install CCM
git clone --branch master --single-branch https://github.com/riptano/ccm.git
pushd ccm || exit
sudo python setup.py install
popd
ccm status || true

export CCM_PATH="$(pwd)/ccm"
export SIMULACRON_PATH="$(pwd)/simulacron/simulacron.jar"

if [ ! -f "$SIMULACRON_PATH" ]; then
    mkdir simulacron
    pushd simulacron
    wget https://github.com/datastax/simulacron/releases/download/0.9.0/simulacron-standalone-0.9.0.jar
    chmod uog+rw simulacron-standalone-0.9.0.jar
    ln -s simulacron-standalone-0.9.0.jar simulacron.jar
    popd
fi
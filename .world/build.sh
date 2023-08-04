#!/bin/bash -ex

. .world/build_config.sh

if [[ "$Linkage" == 'static' ]]; then
  exit
fi

BASEDIR=$(pwd)
WSA_TEST_DB_PATH=$BASEDIR/tests/testdata WSA_TEST_CONFIGURATION_PATH=$BASEDIR/src/bin/test_reports_cfg.yaml cargo test

if [ "$Target" = 'linux' ]; then

  cargo build -r

elif [ "$Target" = 'windows_package' ]; then
  cargo build -r
  ls /home/builder/make_world/world/sidr/target/release/
fi

#!/bin/bash

export JRUBY_OPTS="--disable:did_you_mean -J-Djava.security.egd=file:/dev/./urandom -J-Xmx2g -J-Xms1024m -J-Xmn512m -Xjit.threshold=10 -J-XX:CompileThreshold=10"

export PB_SERVER_TYPE="protobuf/nats/runner"
export PB_CLIENT_TYPE="protobuf/nats/client"

echo "$PWD"

bundle exec ruby -I lib bench/real_client.rb

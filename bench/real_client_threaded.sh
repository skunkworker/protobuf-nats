#!/bin/bash

export JRUBY_OPTS="-J-server --disable:did_you_mean -Xcompile.invokedynamic=true -Xjit.threshold=10 -J-Djava.security.egd=file:/dev/./urandom -J-Xms2g -J-Xmx2g -J-XX:+UseG1GC -J-XX:MaxGCPauseMillis=100"

export PB_SERVER_TYPE="protobuf/nats/runner"
export PB_CLIENT_TYPE="protobuf/nats/client"

export THREAD_COUNT=4

bundle exec ruby -I lib bench/real_client_threaded.rb

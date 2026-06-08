#!/bin/bash

# export JRUBY_OPTS="-J-server --disable:did_you_mean -Xcompile.invokedynamic=true -Xjit.threshold=0 -J-Djruby.jit.max=0 -J-Djruby.jit.background=false -J-Djava.security.egd=file:/dev/./urandom -J-Xms2g -J-Xmx2g -J-XX:+UseG1GC -J-XX:MaxGCPauseMillis=100"

export JRUBY_OPTS="-J-server -J-Xms4g -J-Xmx4g -J-XX:+AlwaysPreTouch -J-XX:+UseParallelGC -J-XX:ReservedCodeCacheSize=768m -J-XX:MaxInlineLevel=18 -J-XX:MaxInlineSize=100 -J-XX:FreqInlineSize=500 -J-XX:LoopUnrollLimit=250 -J-XX:+UseSuperWord -J-Djruby.jit.threshold=0 -J-Djruby.jit.max=0 -J-Djruby.jit.background=false -J-Djruby.inline.all=true -Xcompile.invokedynamic=true"


export PB_SERVER_TYPE="protobuf/nats/runner"
export PB_CLIENT_TYPE="protobuf/nats/client"

export PB_NATS_SERVER_SLOW_START_DELAY=1

export PB_NATS_SERVER_MAX_QUEUE_SIZE=6

bundle exec rpc_server start --threads=10 ./examples/warehouse/app.rb


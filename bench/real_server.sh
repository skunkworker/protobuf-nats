export JRUBY_OPTS="--disable:did_you_mean -J-Djava.security.egd=file:/dev/./urandom -J-Xmx2g -J-Xms1024m -J-Xmn512m -Xjit.threshold=10 -J-XX:CompileThreshold=10"

export PB_SERVER_TYPE="protobuf/nats/runner"
export PB_CLIENT_TYPE="protobuf/nats/client"

export PB_NATS_SERVER_SLOW_START_DELAY=1

export PB_NATS_SERVER_MAX_QUEUE_SIZE=6

bundle exec rpc_server start --threads=2 ./examples/warehouse/app.rb

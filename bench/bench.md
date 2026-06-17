
Notes:
`-Xjit.threshold=0` - Setting the threshold to 0 forces JRuby to compile every method into Java bytecode immediately before its very first execution. This is particularly useful for debugging or bypassing warm-up times during profiling


`-Xjit.threshold=10 -J-XX:CompileThreshold=10` - If you are running benchmarks and want both JRuby and the JVM to aggressively optimize early, you can lower both thresholds simultaneously

`bundle;  bx ruby -I lib bench/real_client.rb`

Start local nats server so details can be monitored.
`/opt/homebrew/opt/nats-server/bin/nats-server -DV -m 8222 -p 4222`


```
export JRUBY_OPTS="--disable:did_you_mean -J-Djava.security.egd=file:/dev/./urandom -J-Xmx2g -J-Xms1024m -J-Xmn512m -Xjit.threshold=10 -J-XX:CompileThreshold=10"
```

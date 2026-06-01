
Ran on Monday, June 1. MBP 14 M1 Pro.

Notes:
`-Xjit.threshold=0` - Setting the threshold to 0 forces JRuby to compile every method into Java bytecode immediately before its very first execution. This is particularly useful for debugging or bypassing warm-up times during profiling


`-Xjit.threshold=10 -J-XX:CompileThreshold=10` - If you are running benchmarks and want both JRuby and the JVM to aggressively optimize early, you can lower both thresholds simultaneously


```
export JRUBY_OPTS="--disable:did_you_mean -J-Djava.security.egd=file:/dev/./urandom -J-Xmx2g -J-Xms1024m -J-Xmn512m -Xjit.threshold=10 -J-XX:CompileThreshold=10"
```


## `jruby-10.0.5.0`

```
I, [2026-06-01T14:37:25.025154 #60447]  INFO -- : Using NATS::Client to connect
jruby 10.0.5.0 (3.4.5) 2026-04-06 5db1ba72f3 OpenJDK 64-Bit Server VM 21.0.11 on 21.0.11 +indy +jit [arm64-darwin]
Warming up --------------------------------------
single threaded performance    16.000 i/100ms
Calculating -------------------------------------
single threaded performance    907.463 (±22.7%) i/s    (1.10 ms/i) -     27.200k in  29.973673s
```

## `jruby-9.4.14.0`

```
I, [2026-06-01T14:35:40.758758 #59092]  INFO -- : Using NATS::Client to connect
jruby 9.4.14.0 (3.1.7) 2025-08-28 ddda6d5992 OpenJDK 64-Bit Server VM 21.0.11 on 21.0.11 +jit [arm64-darwin]
Warming up --------------------------------------
single threaded performance    22.000 i/100ms
Calculating -------------------------------------
single threaded performance      1.014k (±11.2%) i/s  (986.40 μs/i) -     30.404k in  29.990625s
```

## `ruby-3.1.7`

```
I, [2026-06-01T14:38:46.998079 #61611]  INFO -- : Using NATS::Client to connect
ruby 3.1.7p261 (2025-03-26 revision 0a3704f218) [arm64-darwin25]
Warming up --------------------------------------
single threaded performance   111.000 i/100ms
Calculating -------------------------------------
single threaded performance      1.120k (± 6.6%) i/s  (893.04 μs/i) -     33.633k in  30.035636s
```

## `ruby-3.4.9`
```
ruby 3.4.9 (2026-03-11 revision 76cca827ab) +PRISM [arm64-darwin25]
Warming up --------------------------------------
single threaded performance   108.000 i/100ms
Calculating -------------------------------------
single threaded performance      1.107k (± 8.5%) i/s  (903.53 μs/i) -     33.264k in  30.054932s
```


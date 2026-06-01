### JRUBY

`export JRUBY_OPTS="--disable:did_you_mean -J-Djava.security.egd=file:/dev/./urandom -J-Xmx2g -J-Xms1024m -J-Xmn512m"`


#### `jruby-9.4.14.0`
```
jruby 9.4.14.0 (3.1.7) 2025-08-28 ddda6d5992 OpenJDK 64-Bit Server VM 21.0.11 on 21.0.11 +jit [arm64-darwin]
Warming up --------------------------------------
single threaded performance    17.000 i/100ms
Calculating -------------------------------------
single threaded performance    198.890 (±82.0%) i/s    (5.03 ms/i) -      5.967k in  30.001485s
```

#### `jruby-10.0.5.0`
```
jruby 10.0.5.0 (3.4.5) 2026-04-06 5db1ba72f3 OpenJDK 64-Bit Server VM 21.0.11 on 21.0.11 +indy +jit [arm64-darwin]
Warming up --------------------------------------
single threaded performance    13.000 i/100ms
Calculating -------------------------------------
single threaded performance    317.355 (± Inf%) i/s    (3.15 ms/i) -      9.516k in  29.985309s
```

#### `ruby-3.1.7`
```
ruby 3.1.7p261 (2025-03-26 revision 0a3704f218) [arm64-darwin25]
Warming up --------------------------------------
single threaded performance    43.000 i/100ms
Calculating -------------------------------------
single threaded performance    658.836 (±12.0%) i/s    (1.52 ms/i) -     19.780k in  30.022660s
```

#### `ruby-3.4.9`
```
ruby 3.4.9 (2026-03-11 revision 76cca827ab) +PRISM [arm64-darwin25]
Warming up --------------------------------------
single threaded performance    77.000 i/100ms
Calculating -------------------------------------
single threaded performance    808.197 (±10.0%) i/s    (1.24 ms/i) -     24.332k in  30.106520s
```

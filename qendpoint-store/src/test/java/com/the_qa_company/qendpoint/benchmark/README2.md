## Before

### @Fork(value = 1, jvmArgs = { "-Xms4G", "-Xmx4G", "-XX:+AlwaysPreTouch" })
Benchmark                              Mode  Cnt     Score    Error  Units
WikiDataBenchmark.testCountSimpleJoin  avgt   10  1466.804 ± 10.966  ms/op

### @Fork(value = 1, jvmArgs = { "-Xms4G", "-Xmx4G", "-XX:+AlwaysPreTouch" , "-XX:+UseSerialGC"})
Benchmark                              Mode  Cnt     Score     Error  Units
WikiDataBenchmark.testCountSimpleJoin  avgt   10  3315.039 ± 135.082  ms/op

### @Fork(value = 1, jvmArgs = { "-Xms32G", "-Xmx32G", "-XX:+AlwaysPreTouch" })
Benchmark                              Mode  Cnt     Score    Error  Units
WikiDataBenchmark.testCountSimpleJoin  avgt   10  1319.262 ± 25.227  ms/op



## After

### @Fork(value = 1, jvmArgs = { "-Xms4G", "-Xmx4G", "-XX:+AlwaysPreTouch" })
Benchmark                               Mode  Cnt     Score    Error  Units
WikiDataBenchmark.testCountSimpleJoin   avgt   10  1220.977 ± 10.316  ms/op
WikiDataBenchmark.testCountSimpleJoin2  avgt   10  1877.909 ± 22.241  ms/op

### @Fork(value = 1, jvmArgs = { "-Xms4G", "-Xmx4G", "-XX:+AlwaysPreTouch" , "-XX:+UseSerialGC"})
Benchmark                               Mode  Cnt     Score     Error  Units
WikiDataBenchmark.testCountSimpleJoin   avgt   10  2502.995 ±  89.774  ms/op
WikiDataBenchmark.testCountSimpleJoin2  avgt   10  3568.581 ± 665.337  ms/op

### @Fork(value = 1, jvmArgs = { "-Xms32G", "-Xmx32G", "-XX:+AlwaysPreTouch" })
Benchmark                               Mode  Cnt     Score    Error  Units
WikiDataBenchmark.testCountSimpleJoin   avgt   10   958.359 ± 15.146  ms/op
WikiDataBenchmark.testCountSimpleJoin2  avgt   10  1764.693 ± 25.449  ms/op

### GraalVM - @Fork(value = 1, jvmArgs = { "-Xms32G", "-Xmx32G", "-XX:+AlwaysPreTouch" })
Benchmark                               Mode  Cnt     Score    Error  Units
WikiDataBenchmark.testCountSimpleJoin   avgt   10   823.190 ± 11.428  ms/op
WikiDataBenchmark.testCountSimpleJoin2  avgt   10  1231.812 ± 14.117  ms/op

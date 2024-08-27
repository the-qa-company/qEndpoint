### 32 GB of memory, no caching

```
Benchmark                                    (order)  Mode  Cnt   Score    Error  Units
WikiDataDifferentIndexesBenchmark.testCount      POS  avgt    3  70.014 ±  3.764  ms/op
WikiDataDifferentIndexesBenchmark.testCount      OSP  avgt    3  63.579 ± 10.304  ms/op
WikiDataDifferentIndexesBenchmark.testCount      PSO  avgt    3  63.280 ±  3.144  ms/op
WikiDataDifferentIndexesBenchmark.testCount      SOP  avgt    3  63.525 ± 11.067  ms/op
WikiDataDifferentIndexesBenchmark.testCount      OPS  avgt    3  59.152 ±  3.904  ms/op
WikiDataDifferentIndexesBenchmark.testCount  Unknown  avgt    3  63.358 ±  8.699  ms/op
```
Unknown is the "default order".

### 4 GB of memory, no caching
```
Benchmark                                    (order)  Mode  Cnt    Score     Error  Units
WikiDataDifferentIndexesBenchmark.testCount      POS  avgt    3  161.099 ± 137.932  ms/op
WikiDataDifferentIndexesBenchmark.testCount      OSP  avgt    3  142.316 ±   6.770  ms/op
WikiDataDifferentIndexesBenchmark.testCount      PSO  avgt    3  143.392 ±  51.168  ms/op
WikiDataDifferentIndexesBenchmark.testCount      SOP  avgt    3   94.574 ±  14.587  ms/op
WikiDataDifferentIndexesBenchmark.testCount      OPS  avgt    3  121.756 ±  59.288  ms/op
WikiDataDifferentIndexesBenchmark.testCount  Unknown  avgt    3   79.557 ±  21.136  ms/op
```
Unknown is the "default order".


### 4 GB of memory, no caching, longer benchmark run (fork=3, warmup=10, iterations=10)
```
Benchmark                                    (order)  Mode  Cnt    Score    Error  Units
WikiDataDifferentIndexesBenchmark.testCount      POS  avgt   30  161.733 ±  6.663  ms/op
WikiDataDifferentIndexesBenchmark.testCount      OSP  avgt   30  159.620 ±  7.252  ms/op
WikiDataDifferentIndexesBenchmark.testCount      PSO  avgt   30  147.293 ±  8.981  ms/op
WikiDataDifferentIndexesBenchmark.testCount      SOP  avgt   30  104.620 ± 19.548  ms/op
WikiDataDifferentIndexesBenchmark.testCount      OPS  avgt   30  130.582 ± 16.454  ms/op
WikiDataDifferentIndexesBenchmark.testCount  Unknown  avgt   30   77.321 ±  1.669  ms/op
```
Unknown is the "default order".



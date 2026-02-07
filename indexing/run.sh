#!/bin/bash

#export JAVA_OPTIONS="-Xms16G -Xmx16G -XX:+UseParallelGC -XX:+AlwaysPreTouch -XX:+DisableExplicitGC"
#time ./qendpoint-cli-2.5.3/bin/rdf2hdt.sh -index -disk -disklocation ./temp/ -multithread latest-lexemes.nt.gz latest-lexemes.hdt


#export JAVA_OPTIONS="-Xms32G -Xmx32G -XX:+UseParallelGC -XX:+AlwaysPreTouch -XX:+DisableExplicitGC"
#time ./qendpoint-cli-2.5.3/bin/rdf2hdt.sh -disk -index -disklocation ./temp/ -multithread /Users/havardottestad/Documents/wikidata/latest-truthy.nt.gz  latest-truthy.hdt


#[main] . [##########] 100.0 done
#Total Triples ......... 499706958
#Different subjects .... 11412855
#Different predicates .. 10681
#Different objects ..... 115723374
#[main] . [##########] 100.0 done
#[main] . [##########] 100.0 done
#[main] . [##########] 100.0 done
#[main] . [##########] 100.0 done
#[main] . [##########] 100.0 done
#08:33:35.743 [main] INFO  c.t.q.c.triples.impl.BitmapTriples - Count predicates in 1 sec 541 ms 121 us
#[main] . [##########] 100.0 done
#[main] . [##########] 100.0 done
#[main] . [##########] 100.0 done
#08:33:45.935 [main] INFO  c.t.qendpoint.core.hdt.impl.HDTImpl - Index generated and saved in 1 min 34 sec 165 ms 41 us
#[main] . [##########] 100.0 done
#
#real    7m29.917s
#user    30m28.923s
#sys     1m9.650s
#export JAVA_OPTIONS="-Xms32G -Xmx32G -XX:+UseParallelGC -XX:+AlwaysPreTouch -XX:+DisableExplicitGC"
export JAVA_OPTIONS="-Xms32G -Xmx32G -XX:+UseParallelGC -XX:+AlwaysPreTouch -XX:+DisableExplicitGC -XX:+UseCompactObjectHeaders"
#time ./qendpoint-cli-2.5.3/bin/rdf2hdt.sh -index -disk -disklocation ./temp/ -multithread /Users/havardottestad/Documents/wikidata/first_500M_lines_truthy.nt.gz first_500M_lines_truthy.hdt
#time ./qendpoint-cli-2.5.3/bin/rdf2hdt.sh -index -no-recreate -disk -disklocation ./temp/ -multithread /Users/havardottestad/Documents/wikidata/first_500M_lines_truthy.nt.gz first_500M_lines_truthy.hdt
#

#[main] . [##########] 100.0 done
#Total Triples ......... 20754779933
#Different subjects .... 2276125108
#Different predicates .. 60661
#Different objects ..... 3753832879
#Common Subject/Object . 2056458644
#
#real    354m53.017s
#user    1272m56.135s
#sys     303m49.361s
export JAVA_OPTIONS="-Xms64G -Xmx64G -XX:+UseParallelGC -XX:+AlwaysPreTouch -XX:+DisableExplicitGC -XX:+UseCompactObjectHeaders"
time ./qendpoint-cli-2.5.3/bin/rdf2hdt.sh -disk -disklocation ./temp/ -multithread /Users/havardottestad/Documents/wikidata/wikidata-20251208-all-BETA.nt.gz wikidata-20251208-all-BETA.hdt






#[main] . [##        ] 20.00 object index: materialize object counts 3,753,832,879/3,753,832,879 (22,635,677 items/s)
#[main] . [##        ] 29.91 object index: build bitmap 3,718,473,974/3,753,832,879 (110,209,661 items/s)
#[main] . [#######   ] 75.00 object index: materialize buckets 20,754,779,933/20,754,779,933 (44,939,329 items/s)
#[main] . [########  ] 89.99 object index: sort object sublists 20,741,753,871/20,754,779,933 (15,119,869 items/s)
#[main] . [######### ] 99.99 object index: count predicates 12,694,052,743/12,709,051,497 (112,945,455 items/s)
#[stats] mem used/total: 35GB/61GB | gc 60s: 0.00% | up: 01:10:16
#[main] . [######### ] 99.00 Creating Predicate bitmap 12693000000 / 12709051497
#[main] . [######### ] 99.00 Generating predicate references
#[main] . [######### ] 99.00 Generating predicate references
#[main] . [######### ] 99.00 Generating predicate references
#[stats] mem used/total: 41GB/61GB | gc 60s: 10.65% | up: 01:29:16
#Index generated and saved in 1 hour 29 min 16 sec 636 ms 99 us
#
#real    89m25.573s
#user    82m30.482s
#sys     15m27.199s
#time ./qendpoint-cli-2.5.3/bin/rdf2hdt.sh -index -no-recreate -disk -disklocation ./temp/ -multithread /Users/havardottestad/Documents/wikidata/wikidata-20251208-all-BETA.nt.gz wikidata-20251208-all-BETA.hdt


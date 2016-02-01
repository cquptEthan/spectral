#!/bin/sh
echo "add jar to classpath"
echo "run hadoop task"
~/hadoop/bin/hadoop fs -rm -r output
~/hadoop/bin/hadoop jar ~/jar/spectral-1.0-SNAPSHOT-jar-with-dependencies.jar 1 1000 20 15 3 10000 2 4 1145720

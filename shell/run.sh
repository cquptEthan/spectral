#!/bin/sh
echo "add jar to classpath"
echo "run hadoop task"
~/hadoop/bin/hadoop fs -rm -r output
~/hadoop/bin/hadoop jar ~/jar/spectral-1.0-SNAPSHOT-jar-with-dependencies.jar edu.cqupt.spectral.Spectral   input/  output/
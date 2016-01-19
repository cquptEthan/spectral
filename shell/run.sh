#!/bin/sh
echo "add jar to classpath"
export HADOOP_CLASSPATH=~/jar/spectral-1.0-SNAPSHOT.jar
echo "run hadoop task"
~/hadoop/bin/hadoop fs -rm -r output
~/hadoop/bin/hadoop edu.cqupt.spectral.Spectral   input/  output/
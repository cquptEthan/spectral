#!/bin/sh
echo "deploy jar"
export USERNAME=work
echo $USERNAME
export LANG=en_US.UTF-8
echo $LANG
scp ../target/spectral-1.0-SNAPSHOT-jar-with-dependencies.jar work@scmhadoop-1:~/jar/
echo "deploy run.sh"
scp run.sh work@scmhadoop-1:~/jar/
echo "change authority"
ssh work@scmhadoop-1 "chmod 755 ~/jar/run.sh"
echo "start run.sh"
ssh work@scmhadoop-1 "~/jar/run.sh "

#!/bin/bash

source settings.sh
JAVA_CC=$JAVA_HOME/bin/javac
export CLASSPATH=".:gen-java:lib/*"

time=$1

KV_PORT=`shuf -i 10000-10999 -n 1`
echo Port number: $KV_PORT

KV_PORT2=`shuf -i 10000-10999 -n 1`
echo Port number 2: $KV_PORT2

$JAVA_HOME/bin/java -Xmx2g StorageNode `hostname` $KV_PORT $ZKSTRING /$USER &
sleep $time

$JAVA_HOME/bin/java  -Xmx2g StorageNode `hostname` $KV_PORT2 $ZKSTRING /$USER &
sleep $time

# https://stackoverflow.com/questions/11176284/time-condition-loop-in-shell
while true
do
	kill -9 $(lsof -i:$KV_PORT -s TCP:LISTEN -t)
	sleep $time
	$JAVA_HOME/bin/java -Xmx2g StorageNode `hostname` $KV_PORT $ZKSTRING /$USER &
	sleep $time
	kill -9 $(lsof -i:$KV_PORT2 -s TCP:LISTEN -t)
	sleep $time
	$JAVA_HOME/bin/java -Xmx2g StorageNode `hostname` $KV_PORT2 $ZKSTRING /$USER &
	sleep $time
done

	

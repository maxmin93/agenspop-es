#!/bin/bash

targetpath="backend/target"
jarfile=`ls $targetpath/agenspop*.jar`
#echo $jarfile
cfgfile=`ls $targetpath/*.yml`
cfgname="${cfgfile%.yml}"
#echo $cfgfile : $cfgname

if [ ! -f $jarfile ]; then
  echo "ERROR: not exist agenspop jar file in ./target/ \nTry build and start again.." >&2;
  exit 1;
fi

echo "Run target jar: $jarfile ($cfgname)"
nohup java -Xms2g -Xmx2g -jar $jarfile --spring.config.name=$cfgname > $targetpath/agenspop.log 2>&1 &

echo "** wait backend..."
sleep 5

cd frontend/src/main/frontend
ng serve --disable-host-check > ../../../../$targetpath/ng-serve.log 2>&1 &
cd ../../../..

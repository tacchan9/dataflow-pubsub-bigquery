#!/bin/bash

if [ "$#" -ne 2 ]; then
   echo "Usage:   ./run_df.sh project-name  bucket-name"
   exit
fi

PROJECT=$1
BUCKET=$2
MAIN=com.StreamDemoConsumer

echo "project=$PROJECT  bucket=$BUCKET  main=$MAIN"

export PATH=/usr/lib/jvm/java-8-openjdk-amd64/bin/:$PATH
mvn compile -e exec:java \
 -Dexec.mainClass=$MAIN \
      -Dexec.args="--project=$PROJECT \
      --stagingLocation=gs://$BUCKET/staging/ \
      --tempLocation=gs://$BUCKET/staging/ \
      --output=$PROJECT:demos.streamdemo \
      --input=projects/$PROJECT/topics/streamdemo \
      --jobName=jobtest \
      --update \
      --runner=DataflowRunner"

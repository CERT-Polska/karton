#!/bin/bash
DOCKERHUB="${DOCKERHUB:=localhost:5000}"

sudo docker build -t $1 .

sudo docker tag $1 $DOCKERHUB/$1
sudo docker push $DOCKERHUB/$1

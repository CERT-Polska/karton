#!/bin/bash
DOCKERHUB="${DOCKERHUB:=localhost:5000}"
DOCKERFILE="Dockerfile"

sudo docker build -t $DOCKERHUB/$1:`git rev-parse HEAD` -f $DOCKERFILE .

sudo docker push $DOCKERHUB/$1:`git rev-parse HEAD`

#sudo kubectl delete -f $1-deployment.yml
#sudo kubectl apply -f $1-deployment.yml


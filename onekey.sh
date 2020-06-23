#! /bin/bash

if (($# == 0))
then
  VERSION=v1
else
  VERSION=$1
fi

JAR_PATH="/Users/tinggao/project/java/tail-based-sampling/target/tail-based-sampling-1.0-SNAPSHOT-jar-with-dependencies.jar"
DOCKER_PATH="/Users/tinggao/project/java/tianchi/docker/"

docker stop $(docker ps -a -q)
docker rmi registry.cn-shanghai.aliyuncs.com/iwant_tianchi/tail-based-sampling:$VERSION

cp $JAR_PATH $DOCKER_PATH
docker build -t registry.cn-shanghai.aliyuncs.com/iwant_tianchi/tail-based-sampling:$VERSION $DOCKER_PATH

# docker login --username=pianpianqiwu_2014 registry.cn-shanghai.aliyuncs.com # 第一次 push 时需要登陆
docker push registry.cn-shanghai.aliyuncs.com/iwant_tianchi/tail-based-sampling:$VERSION


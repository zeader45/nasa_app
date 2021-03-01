# Spark Cluster with Docker & docker-compose

# General

A simple spark standalone cluster for your testing environment purpose.

The Docker compose will create the following containers:

container|Ip address
---|---
spark-master|10.5.0.2
spark-worker-1|10.5.0.3
spark-worker-2|10.5.0.4
spark-worker-3|10.5.0.5

# Steps to run the app

Need to run following steps to see final results according to the assignment requirement.

## Pre requisites

* Docker installed

* Docker compose  installed

## Build the images

The first step to deploy the cluster will be the build of the custom images, these builds can be performed with the *build-images.sh* script. 

The executions is as simple as the following steps:

```sh
chmod +x build-images.sh
./build-images.sh
```

This will create the following docker images:

* spark-base:2.4.3: A base image based on java:alpine-jdk-8 wich ships scala, python3 and spark 2.4.7

* spark-master:2.4.3: A image based on the previously created spark image, used to create a spark master containers.

* spark-worker:2.4.3: A image based on the previously created spark image, used to create spark worker containers.

* spark-submit:2.4.3: A image based on the previously created spark image, used to create spark submit containers(run, deliver driver and die gracefully).

## Run the docker-compose

The final step to create your cluster will be to run the compose file:

```sh
docker-compose up --scale spark-worker=3
```


# Binded Volumes

To make app running easier I've shipped two volume mounts described in the following chart:

Host Mount|Container Mount|Purposse
---|---|---
/mnt/spark-apps|/opt/spark-apps|Used to make available your app's jars on all workers & master
/mnt/spark-data|/opt/spark-data| Used to make available your app's data on all workers & master


## Run nasa_app and show results

from the project root directory, run 
```sh
docker exec -it nasa_app_spark-master_1 spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --driver-memory 2g --class NasaDataProcessor  /opt/spark-apps/nasa_app_2.11-0.1.jar  5
```
The last argument can be changed to any number you want which stands for TopN.

## Note
The .jar file is saved in nasa_app/apps/
#!/bin/bash

echo "start-cluster script"


#Set workers containers for cluster from arguments, default = 1
set_workers() {
  if [ -z "$1" ]
    then
      workersCount=1
    else
      workersCount=$1
  fi
  echo "$workersCount worker and 1 master container will be created..."
}

#Clean workers config file, update it with the required workers docker-containers names
set_workers_file(){
  : > ./config/workers
  for (( i=1; i<=$workersCount; i++ ))
    do
        echo "Exporting worker$i to workers file..."
        echo "hadoop-worker$i" >> ./config/workers
    done
}

#Create a network named "hadoopNetwork"
create_network() {
  docker network create -d bridge  --subnet 172.25.0.0/16  hadoopNetwork
}

#Create base hadoop image named "hadoop-base:1.0"
build_image(){
  base_image="hadoop-base:1.0"
  docker build -t $base_image .
}


#Create master and worker containers, add them to the network
#Exposed ports: 8088-WebUI for Yarn Resource Manager, 9870-WebUI for NameNode, 9868-WebUI for Secondary NameNode
create_cluster(){
  #run $base_image image as master container
  docker run -itd  --network="hadoopNetwork"  --ip 172.25.0.100  -p 8088:8088 -p 9870:9870 -p 9868:9868 --name hadoop-master --hostname hadoop-master  $base_image

  #run $base_image as worker container
  for (( c=1; c<=$workersCount; c++ ))
    do
        tmpName="hadoop-worker$c"
        docker run -itd  --network="hadoopNetwork" --name $tmpName --hostname $tmpName  $base_image
    done
}

#run hadoop stop and start commands & create directories
run_cluster_commands(){
  docker exec -ti hadoop-master bash  -c "/usr/local/hadoop/sbin/stop-all.sh && hadoop namenode -format && /usr/local/hadoop/sbin/start-all.sh"
  docker exec -ti hadoop-master bash  -c "mkdir /program; mkdir /dataFolder;"
  docker exec -i hadoop-master bash  -c "mapred --daemon start historyserver"
  
}


move_data_jar(){
  mydir=${0:a:h}
  SCRIPT_DIR=$(cd $(dirname "${BASH_SOURCE[0]}") && pwd)
  docker cp  $SCRIPT_DIR/dataFolder/clean_data.csv hadoop-master:/dataFolder
  docker cp  $SCRIPT_DIR/program/BDM_Project1-1.0.60.jar hadoop-master:/program
  docker cp  $SCRIPT_DIR/pigScripts/ hadoop-master:/pigScripts
}


add_input_hdfs(){
  docker exec -ti hadoop-master bash  -c "hadoop fs -mkdir -p input"
  docker exec -ti hadoop-master bash  -c "hdfs dfs -put ./dataFolder/* input"
}


#Run functions by order
set_workers $*
set_workers_file
create_network
build_image
create_cluster
run_cluster_commands
move_data_jar
add_input_hdfs






#docker cp  /Users/george/Documents/MSc-Data-Science/Big-Data-Management/clean_data.csv hadoop-master:/dataFolder
#docker cp  /Users/george/Documents/IdeaProjects/BDM_Project1/target/BDM_Project1-1.0.60.jar hadoop-master:/program








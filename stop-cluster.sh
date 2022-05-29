#!/bin/bash
echo "stop-cluster script"

destroy_cluster() {

  #docker exec -ti hadoop-master bash  -c ""
  docker kill hadoop-master; docker rm hadoop-master
  cat config/workers | while read line 
    do
       docker kill $line; docker rm $line
    done

  docker network rm hadoopNetwork
}


#Run function
destroy_cluster
#!/bin/bash


docker exec -ti hadoop-master bash  -c "pig /pigScripts/query_2.pig"
docker exec -ti hadoop-master bash  -c "pig /pigScripts/query_3.pig"
docker exec -ti hadoop-master bash  -c "pig /pigScripts/query_4.pig"

docker exec -ti hadoop-master bash  -c "hadoop jar /program/BDM_Project1-1.0.60.jar Vitsas_Spiliakos.Question2 input JavaResults/task2"
docker exec -ti hadoop-master bash  -c "hadoop jar /program/BDM_Project1-1.0.60.jar Vitsas_Spiliakos.Question3 input output1 output2 output3 JavaResults/task3"
docker exec -ti hadoop-master bash  -c "hadoop jar /program/BDM_Project1-1.0.60.jar Vitsas_Spiliakos.Question4 input output1 output2 JavaResults/task4"
   


#!/bin/bash

Help()
{
   # Display Help
   echo "Description"
   echo "Syntax: ./export_results [h|j|p|d]"
   echo "Options:"
   echo "h     Print this Help."
   echo "j     set 'j true' if you want to parse JavaResults."
   echo "p     set 'p true' if you want to parse PigResults."
   echo "d     directory path to save results on your local machine."
   echo
   echo "You must provide d option and one or both of j/p options with value true."
   echo "You must have executed the steps 6 for p option and 7 for j option before exporting. "
   echo
   echo "Example: ./export_results.sh -j true d /path/for/results/on/local/"
   echo "Example: ./export_results.sh -p true d /path/for/results/on/local/"
   echo "Example: ./export_results.sh -j true p true d /path/for/results/on/local/"
   echo
}


# Get the options
while getopts ":hj:p:d:" option; do
   case $option in
        h) # display Help
            Help
            exit;;
        j) # Enter a name
            java=$OPTARG;;
        p) # Enter a name
            pig=$OPTARG;;
        d) # Enter a name
            path=$OPTARG;;
     \?) # Invalid option
         echo "Error: Invalid option"
         exit;;
   esac
done



if [ -z "$path" ] 
then 
    echo "Please provide -path path/to/save/results/ argument.";
    exit;
else
    docker exec -ti hadoop-master bash  -c "rm -r /results; mkdir /results"
    if [ "$java" != "true" ] && [ "$pig" != "true" ] 
    then
        echo 'You need to set -pig="true" or -java="true" or both.'
        exit;
    else
        if [ "$java" == 'true' ] 
        then
            docker exec -i hadoop-master bash  -c "hdfs dfs -cat /user/root/JavaResults/task2/part-r-00000 > /results/Java_task2.csv;"
            docker exec -i hadoop-master bash  -c "hdfs dfs -cat /user/root/JavaResults/task3/part-r-00000 > /results/Java_task3.csv;"
            docker exec -i hadoop-master bash  -c "hdfs dfs -cat /user/root/JavaResults/task4/part-r-00000 > /results/Java_task4.txt;"
        fi
        if [ "$pig" == 'true' ] 
        then
            docker exec -i hadoop-master bash  -c "hdfs dfs -cat /user/root/PigResults/task2/part-r-00000 > /results/Pig_task2.csv;"
            docker exec -i hadoop-master bash  -c "hdfs dfs -cat /user/root/PigResults/task3/part-m-00000 > /results/Pig_task3.csv;"
            docker exec -i hadoop-master bash  -c "hdfs dfs -cat /user/root/PigResults/task4/part-r-00000 > /results/Pig_task4.txt;"
        fi
    fi

    docker cp hadoop-master:/results/ $path
fi



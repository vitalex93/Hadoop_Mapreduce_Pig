FROM ubuntu:16.04


RUN apt-get update
RUN ln -snf /usr/share/zoneinfo/Europe/Athens /etc/localtime && echo Europe/Athens > /etc/timezone
RUN apt-get install openjdk-8-jdk -y
RUN apt-get install -y wget
RUN apt-get install -y openssh-server
RUN wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.2/hadoop-3.3.2.tar.gz
RUN tar zxvf hadoop-3.3.2.tar.gz 
RUN mv hadoop-3.3.2 /usr/local/hadoop 
RUN rm hadoop-3.3.2.tar.gz

RUN mkdir -p /pig
RUN wget https://downloads.apache.org/pig/pig-0.17.0/pig-0.17.0.tar.gz
RUN tar zxvf pig-0.17.0.tar.gz
RUN mv pig-0.17.0 /pig
RUN rm pig-0.17.0.tar.gz


#RUN mkdir -p /datafu
#RUN wget https://dlcdn.apache.org/datafu/apache-datafu-1.6.1/apache-datafu-sources-1.6.1.tgz
#RUN tar zxvf apache-datafu-sources-1.6.1.tgz
#RUN mv apache-datafu-sources-1.6.1 /datafu
#RUN rm apache-datafu-sources-1.6.1.tgz
#
#RUN apt-get install unzip
#
#RUN wget -q https://services.gradle.org/distributions/gradle-4.5.1-bin.zip \
#    && unzip gradle-4.5.1-bin.zip -d /opt \
#    && rm gradle-4.5.1-bin.zip
#
## Set Gradle in the environment variables
#ENV GRADLE_HOME /opt/gradle-4.5.1
#ENV PATH $PATH:/opt/gradle-4.5.1/bin



#ENV  JAVA_HOME /usr/lib/jvm/java-8-openjdk-arm64/jre
#RUN echo JAVE_HOME=$(readlink -f /usr/bin/java | sed "s:/bin/java::") >> /tmpvariable.txt
#RUN JAVA_HOME=$(cat /tmpvariable.txt);
ENV JAVA_HOME $(readlink -f /usr/bin/java | sed "s:/bin/java::")

ENV  PATH $PATH:$JAVA_HOME/bin
ENV  HADOOP_HOME /usr/local/hadoop
ENV  PATH $PATH:$HADOOP_HOME/bin
ENV  HADOOP_CONF_DIR $HADOOP_HOME/etc/hadoop


ENV PIG_HOME /pig/pig-0.17.0
ENV PATH $PATH:$PIG_HOME/bin
ENV PIG_CLASSPATH = $HADOOP_HOME/conf


RUN ssh-keygen -t rsa -f ~/.ssh/id_rsa -P '' && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys


RUN mkdir -p ~/hdfs/namenode && \ 
    mkdir -p ~/hdfs/datanode && \
    mkdir $HADOOP_HOME/logs

COPY config/* /tmp/

RUN mv /tmp/ssh_config ~/.ssh/config && \
    mv /tmp/hdfs-site.xml $HADOOP_CONF_DIR/hdfs-site.xml && \ 
    mv /tmp/core-site.xml $HADOOP_CONF_DIR/core-site.xml && \
    mv /tmp/mapred-site.xml $HADOOP_CONF_DIR/mapred-site.xml && \
    mv /tmp/yarn-site.xml $HADOOP_CONF_DIR/yarn-site.xml && \
    mv /tmp/workers $HADOOP_CONF_DIR/workers 


RUN echo "export JAVA_HOME=${JAVA_HOME}" >> $HADOOP_CONF_DIR/hadoop-env.sh
RUN echo "export HADOOP_HOME=${HADOOP_HOME}" >> $HADOOP_CONF_DIR/hadoop-env.sh
RUN echo "export HADOOP_CONF_DIR=${HADOOP_CONF_DIR}" >> $HADOOP_CONF_DIR/hadoop-env.sh

RUN echo "export HDFS_NAMENODE_USER=root" >> $HADOOP_CONF_DIR/hadoop-env.sh
RUN echo "export HDFS_DATANODE_USER=root" >> $HADOOP_CONF_DIR/hadoop-env.sh
RUN echo "export HDFS_SECONDARYNAMENODE_USER=root" >> $HADOOP_CONF_DIR/hadoop-env.sh

RUN echo "export YARN_RESOURCEMANAGER_USER=root" >> $HADOOP_CONF_DIR/yarn-env.sh
RUN echo "export YARN_NODEMANAGER_USER=root" >> $HADOOP_CONF_DIR/yarn-env.sh



CMD [ "sh", "-c", "service ssh start; bash"]

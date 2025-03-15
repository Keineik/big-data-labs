FROM ubuntu:22.04

# Install packages
RUN \
  apt-get update && apt-get install -y \
  sudo \
  ssh \
  rsync \
  nano \
  openjdk-8-jdk

# Download hadoop
WORKDIR /opt
RUN \
  wget https://dlcdn.apache.org/hadoop/common/hadoop-3.4.1/hadoop-3.4.1-lean.tar.gz && \
  tar -xzf hadoop-3.4.1-lean.tar.gz && \
  mv hadoop-3.4.1 /opt/hadoop

# Set env variables
ENV HADOOP_HOME=/opt/hadoop
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
ENV HDFS_NAMENODE_USER=root
ENV HDFS_DATANODE_USER=root
ENV HDFS_SECONDARYNAMENODE_USER=root

RUN \
  echo "export HADOOP_HOME=$HADOOP_HOME" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh && \
  echo "export JAVA_HOME=$JAVA_HOME" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh && \
  echo "export PATH=\${JAVA_HOME}/bin:\${PATH}" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh && \
  echo "export PATH=\$PATH:\${HADOOP_HOME}/bin" >> ~/.bashrc && \
  echo "export PATH=\${JAVA_HOME}/bin:\${PATH}" >> ~/.bashrc && \
  echo "export HADOOP_CLASSPATH=\${JAVA_HOME}/lib/tools.jar" >> ~/.bashrc

WORKDIR /opt/hadoop
# create ssh keys
RUN \
  ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
  cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
  chmod 0600 ~/.ssh/authorized_keys

# copy hadoop configs
COPY configs/*xml etc/hadoop/
# copy ssh config
COPY configs/ssh_config /root/.ssh/config

# expose ports
EXPOSE 9870 9000

ENTRYPOINT [ "bash", "-c", "service ssh restart \
    && $HADOOP_HOME/bin/hdfs namenode -format \
    && $HADOOP_HOME/sbin/start-dfs.sh \
    && bash" \
]
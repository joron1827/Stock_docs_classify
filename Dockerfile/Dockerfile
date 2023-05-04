FROM python:3.8-slim-buster

# Install dependencies
RUN apt-get update && \
    apt-get install -y \
        default-jdk \
        build-essential \
        libkrb5-dev \
        libssl-dev \
        libffi-dev \
        libpq-dev \
        wget \
        curl \
        git \
        && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
ENV HADOOP_HOME /usr/local/hadoop
ENV ARROW_LIBHDFS_DIR /usr/local/hadoop/lib/native



COPY ./requirements.txt ./
COPY ./hadoop-3.3.2.tar.gz ./

RUN tar -xzf hadoop-3.3.2.tar.gz && \
    rm hadoop-3.3.2.tar.gz && \
    mv hadoop-3.3.2 /usr/local/hadoop


# Download and install Hadoop
#RUN wget https://downloads.apache.org/hadoop/common/hadoop-3.3.2/hadoop-3.3.2.tar.gz && \
#    tar -xzf hadoop-3.3.2.tar.gz && \
#    rm hadoop-3.3.2.tar.gz && \
#    mv hadoop-3.3.2 /usr/local/hadoop

# Add Hadoop and PyArrow to the CLASSPATH
ENV CLASSPATH $HADOOP_HOME/etc/hadoop:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/tools/lib/*:$ARROW_LIBHDFS_DIR:/usr/local/lib/python3.8/site-packages/pyarrow/lib

# Install PyArrow
RUN pip install --upgrade pip
RUN pip install psycopg2
RUN pip install -r requirements.txt

# Set the entrypoint to start a bash shell
ENTRYPOINT ["/bin/bash"]


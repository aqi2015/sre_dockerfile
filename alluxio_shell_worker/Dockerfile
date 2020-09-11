# docker build -t alluxio-shell-worker .

FROM centos7:latest
# FROM alluxio-shell-base:latest
USER root
WORKDIR /

ADD alluxio-2.2.1-bin.tar.gz /
ADD jdk-8u151-linux-x64.tar.gz /
RUN ln -s /alluxio-2.2.1 alluxio \
    && ln -s /jdk1.8.0_151 jdk \
    && yum install -y python3
ENV JAVA_HOME /jdk
ENV PATH $JAVA_HOME/bin:$PATH
ENV ALLUXIO_HOME /alluxio
ENV PATH $ALLUXIO_HOME/bin:$PATH
COPY alluxio-site.properties workers /alluxio/conf/
COPY alluxio_worker.py  client.py requirements.txt /

# RUN pip3 install -r requirements.txt

# ENTRYPOINT ["python3", "/alluxio_worker.py"]


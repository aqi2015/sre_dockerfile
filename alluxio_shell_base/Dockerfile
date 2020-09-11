# docker build -t alluxio-shell-base .

FROM centos7:latest
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
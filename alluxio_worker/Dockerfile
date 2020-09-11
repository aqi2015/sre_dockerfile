# docker build -t alluxio-worker .
#FROM centos
#FROM python:3
FROM python:alpine3.6

USER root

WORKDIR /

#RUN yum -y install python3 python3-pip curl wget telnet
#RUN yum -y install python3 python3-pip

COPY * /

RUN pip3 install -r requirements.txt 

ENTRYPOINT ["python3", "/alluxio_worker.py"]

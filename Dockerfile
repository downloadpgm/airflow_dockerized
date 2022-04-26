FROM ubuntu:18.04

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=US/Central

RUN apt-get update && apt-get install -y supervisor vim iputils-ping python3-pip

RUN pip3 install apache-airflow
RUN pip3 install apache-airflow-providers-apache-spark

WORKDIR /root
RUN airflow db init
RUN airflow users create --username admin --firstname marcelok --lastname marcelok --role Admin --email mkenjis@gmail.com --password xxxxxx

COPY run_airflow.sh .
RUN chmod +x run_airflow.sh
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

EXPOSE 8080

CMD ["/usr/bin/supervisord"]

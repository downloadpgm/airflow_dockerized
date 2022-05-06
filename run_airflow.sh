trap "{ echo Stopping play app; /root/stop_airflow.sh; exit 0; }" SIGTERM

airflow webserver -D

airflow scheduler

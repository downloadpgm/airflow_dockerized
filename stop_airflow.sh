WEBSVR_ID=`ps -ef | grep "airflow webserver" | grep -wv grep | awk '{ print $1 }'`
SCHED_ID=`ps -ef | grep "airflow scheduler" | grep python | grep -wv grep | awk '{ print $1 }'`

KILL -TERM ${WEBSVR_ID} ${SCHED_ID}

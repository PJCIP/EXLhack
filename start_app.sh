NUM_WORKERS=6
TIMEOUT=120
NAME=Storage
PIDFILE=pid

gunicorn app:app \
--name $NAME \
--workers $NUM_WORKERS \
--timeout $TIMEOUT \
--log-level=debug \
--bind=0.0.0.0:5000 \
--pid=$PIDFILE \
-D

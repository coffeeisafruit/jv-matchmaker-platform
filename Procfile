web: gunicorn config.wsgi --bind 0.0.0.0:$PORT
worker: prefect worker start --pool railway-pool --type process
release: python manage.py migrate --noinput

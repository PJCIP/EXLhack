FROM python:3.8

WORKDIR /app
COPY . .

RUN pip install -r requirements.txt
EXPOSE 8000
# ENTRYPOINT ["gunicorn","app:app"]
CMD ["gunicorn"  , "--bind", "0.0.0.0:8000", "app:app"]

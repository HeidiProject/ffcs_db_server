FROM tiangolo/uvicorn-gunicorn-fastapi:python3.11

COPY ./app /app

RUN pip install pymongo python-dateutil

EXPOSE 8000

CMD ["uvicorn", "ffcs_db_server:app", "--host", "0.0.0.0", "--port", "8081", "--ssl-keyfile", "/app/key.pem", "--ssl-certfile", "/app/cert.pem"]

# Dockerfile
FROM python:3.9-slim

RUN pip install --upgrade pip
RUN pip install pandas sqlalchemy psycopg2-binary requests pyarrow

WORKDIR /app

COPY ingest_data.py ingest_data.py

ENTRYPOINT [ "python" , "ingest_data.py" ]
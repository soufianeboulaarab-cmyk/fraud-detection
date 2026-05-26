FROM apache/spark:3.5.0-python3

USER root

RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    kafka-python==2.0.2 \
    psycopg2-binary==2.9.9 \
    pandas==2.1.3 \
    numpy==1.26.2 \
    requests==2.31.0

COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app
COPY . /app/

USER spark

CMD ["python", "/app/spark_streaming_pipeline.py"]

FROM python:3.11-slim

WORKDIR /app
RUN pip install kafka-python

COPY simulate_transactions.py .

CMD ["python", "simulate_transactions.py"]

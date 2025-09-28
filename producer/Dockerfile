FROM python:3.10-slim

WORKDIR /app

COPY producer.py .

# Install dependencies including sseclient-py
RUN pip install --no-cache-dir kafka-python requests sseclient-py

CMD ["python", "producer.py"]

FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir kafka-python requests
COPY producer.py .
CMD ["python", "producer.py"]

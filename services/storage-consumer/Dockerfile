# Owner: Person B — build context is ./services (see docker-compose.yml)
FROM python:3.12-slim
WORKDIR /app
COPY storage-consumer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common /app/common
COPY storage-consumer/consumer.py .
CMD ["python", "-u", "consumer.py"]

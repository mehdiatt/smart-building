# Owner: Person B — build context is ./services (see docker-compose.yml)
FROM python:3.12-slim
WORKDIR /app
COPY fake-producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common /app/common
COPY fake-producer/producer.py .
CMD ["python", "-u", "producer.py"]

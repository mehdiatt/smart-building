# Owner: Person B — build context is ./services (see docker-compose.yml)
FROM python:3.12-slim
WORKDIR /app
COPY aggregator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common /app/common
COPY aggregator/aggregator.py .
CMD ["python", "-u", "aggregator.py"]

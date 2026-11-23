# Owner: Person B — build context is ./services (see docker-compose.yml)
FROM python:3.12-slim
WORKDIR /app
COPY notifier/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common /app/common
COPY notifier/notifier.py .
CMD ["python", "-u", "notifier.py"]

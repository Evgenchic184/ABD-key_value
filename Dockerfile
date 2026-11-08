FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY kvdb ./kvdb
CMD ["python", "-m", "kvdb.http", "--host", "0.0.0.0", "--data-dir", "/data"]

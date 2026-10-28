FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Server-only runtime dependencies. This intentionally excludes the optional
# FR24 client package placeholder from requirements.txt.
RUN pip install --no-cache-dir \
    duckdb \
    fastapi \
    pandas \
    pymysql \
    SQLAlchemy \
    uvicorn

COPY . /app

RUN mkdir -p /app/duckdb /app/logs

CMD ["python", "fr24_server.py"]

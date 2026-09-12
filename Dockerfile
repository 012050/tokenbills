FROM python:3.12-slim

WORKDIR /app

ENV TOKENBILLS_DB=/app/data/tokenbills.db \
    PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir fastapi httpx uvicorn

COPY main.py ./
COPY static/ ./static/

RUN mkdir -p /app/data

EXPOSE 8000
VOLUME /app/data

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--no-proxy-headers"]

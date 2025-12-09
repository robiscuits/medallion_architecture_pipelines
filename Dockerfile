FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    STATSAPI_INPUT_ROOT=/workspace/data \
    STATSAPI_OUTPUT_ROOT=/workspace/bronze

WORKDIR /app

RUN pip install --no-cache-dir pandas pyarrow duckdb

COPY statsapi_json_flattening.py /app/statsapi_json_flattening.py

ENTRYPOINT ["python", "/app/statsapi_json_flattening.py"]

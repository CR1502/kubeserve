# Baseline: the same model served by plain Hugging Face transformers behind FastAPI.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app
RUN pip install --index-url https://download.pytorch.org/whl/cpu torch==2.13.0
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY app.py .

EXPOSE 8000
# One process, one worker: no replication or batching inside the pod.
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]

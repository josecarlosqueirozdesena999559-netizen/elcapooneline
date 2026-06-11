FROM python:3.11-slim

WORKDIR /app

COPY backend/requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /app/requirements.txt

COPY backend /app/backend

EXPOSE 8080

CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8080"]

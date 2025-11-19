FROM python:3.10-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Use env PORT (Cloud Run sets $PORT). Default 8000 for local testing.
ENV PORT=8080
EXPOSE ${PORT}

# Use uvicorn as entrypoint
CMD ["sh", "-c", "uvicorn main_fastapi:app --host 0.0.0.0 --port ${PORT}"]

FROM python:3.10-slim

WORKDIR /app

# Copy backend code
COPY backend/ ./backend/

# Copy ingestion code
COPY ingestion/ ./ingestion/

# Install dependencies
RUN pip install fastapi uvicorn requests python-dotenv

WORKDIR /app/backend

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

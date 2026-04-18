# Use lightweight Python base image
FROM python:3.10-slim

# Prevent Python from writing pyc files & enable logs immediately
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Copy only requirements first (better caching)
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy ingestion code
COPY extractors/ ./extractors/

# Create data directory (for volume mount)
RUN mkdir -p /app/data

# Default entrypoint
ENTRYPOINT ["python", "extractors/fetch_prices.py"]

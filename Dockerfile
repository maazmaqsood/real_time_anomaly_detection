# Use Python 3.9 slim image as base
FROM python:3.9-slim

# Install system dependencies including netcat for health checks
RUN apt-get update && apt-get install -y \
    netcat-traditional \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install -r requirements.txt

# Copy the rest of the application
COPY . .

# Make sure scripts are executable
RUN chmod +x entrypoint-kafka.sh

# Set Python to run in unbuffered mode
ENV PYTHONUNBUFFERED=1

# Use entrypoint script to control execution
ENTRYPOINT ["/bin/bash", "/app/entrypoint-kafka.sh"]
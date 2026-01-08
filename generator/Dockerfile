FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY transaction_generator.py .

# Default command
CMD ["python", "transaction_generator.py", "--bootstrap-servers", "redpanda:9092"]

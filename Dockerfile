# Use Python 3.12 slim image
FROM python:3.12-slim

# Install build dependencies for TgCrypto
RUN apt-get update && apt-get install -y gcc python3-dev && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the bot code
COPY image_downloader_bot.py .

# Expose port for FastAPI health check
EXPOSE 10001

# Run the bot
CMD ["python", "image_downloader_bot.py"]

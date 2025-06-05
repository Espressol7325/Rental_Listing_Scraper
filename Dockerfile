# Use lightweight Python base
FROM python:3.10-slim

# Install Chromium and dependencies
RUN apt-get update && apt-get install -y \
    chromium-driver chromium wget unzip curl \
    fonts-liberation libappindicator3-1 libasound2 libatk-bridge2.0-0 \
    libatk1.0-0 libcups2 libdbus-1-3 libgdk-pixbuf2.0-0 libnspr4 \
    libnss3 libxcomposite1 libxdamage1 libxrandr2 xdg-utils \
    --no-install-recommends && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy only requirements first for better Docker cache usage
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your app code (optional, will be overwritten by volume in compose)
COPY . .

# Default command (can be overridden by docker-compose)
CMD ["python", "Scrapping_Web.py"]

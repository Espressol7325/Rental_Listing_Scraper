FROM python:3.10-slim

# Force unbuffered logs
ENV PYTHONUNBUFFERED=1

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

# Copy dependencies first for better cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy entire project
COPY . .

# Environment vars (you can still set them in Railway Dashboard too)
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver

CMD ["python", "-u", "Scrapping_Web.py"]

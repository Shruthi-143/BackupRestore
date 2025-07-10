FROM python:3.11-slim

WORKDIR /app

COPY . .
COPY requirements.txt /app/requirements.txt
COPY .env /app/

RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    build-essential \
    python3-venv \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m venv /app/venv \
    && /app/venv/bin/pip install --upgrade pip \
    && /app/venv/bin/pip install --no-cache-dir -r /app/requirements.txt

EXPOSE 8000

CMD ["/app/venv/bin/python", "manage.py", "runserver", "0.0.0.0:8000"]

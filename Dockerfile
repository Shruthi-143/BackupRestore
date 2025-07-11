FROM python:3.11-slim

WORKDIR /app

COPY . .
COPY requirements.txt /app/requirements.txt
COPY .env /app/

RUN apt-get update && apt-get install -y \
    postgresql-client \
    gcc \
    libpq-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt  

EXPOSE 8000

CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]

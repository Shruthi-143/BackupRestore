FROM alpine/python:3.12
WORKDIR /app
COPY . .
COPY requirements.txt /app/requirements.txt
COPY .env /app/
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*
RUN apk --no-cache add python3 py3-pip py3-virtualenv
RUN python3 -m venv /app/venv \
  && . /app/venv/bin/activate \
  && pip install --no-cache-dir -r /app/requirements.txt
EXPOSE 8000
CMD ["/app/venv/bin/python", "manage.py", "runserver", " ,"0.0.0.0:8000"]
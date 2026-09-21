FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    API_HOST=0.0.0.0

WORKDIR /app
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py ./main.py
COPY application ./application
COPY config ./config

VOLUME ["/app/data"]
EXPOSE 8080
CMD ["python", "main.py"]

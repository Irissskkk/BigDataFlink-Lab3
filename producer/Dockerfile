FROM python:3.10-slim

WORKDIR /app

# Устанавливаем зависимости
RUN pip install kafka-python pandas faker

COPY producer.py .

CMD ["python", "-u", "producer.py"]

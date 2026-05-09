FROM python:3.10-slim

WORKDIR /app

# Устанавливаем зависимости
RUN pip install kafka-python pandas faker

# Копируем скрипт
COPY producer.py .

# Запускаем
CMD ["python", "-u", "producer.py"]
# Лабораторная работа №3: Streaming processing с помощью Flink

## Структура репозитория

BigDataFlink/
├── data/ # Исходные данные (10 CSV файлов по 1000 строк)
├── screenshots/ # Скриншоты результатов
│ ├── screenshot1_count.jpg # 10000 записей
│ ├── screenshot2_data.jpg # Пример данных
│ ├── screenshot3_categories.jpg # Статистика по категориям
│ └── screenshot4_top10.jpg # Топ-10 покупок
├── docker-compose.yml # Docker Compose с PostgreSQL, Kafka, Flink
├── producer/
│ ├── Dockerfile # Docker образ для producer
│ └── producer.py # Отправка CSV в Kafka (10 000 сообщений)
├── flink-job/
│ ├── pom.xml # Maven зависимости
│ └── src/main/java/com/flink/
│ ├── SaleEvent.java # Модель данных
│ └── FlinkStarTransformer.java # Flink задача (streaming)
├── init-db/
│ └── init.sql # Создание таблицы fact_sales
└── README.md 

## Выполненные требования

### 1. Исходные данные
Создано 10 CSV файлов (mock_data1.csv - mock_data10.csv)
Каждый файл содержит 1000 строк с данными о продажах
Всего сгенерировано 10 000 записей

### 2. Docker Compose
Файл docker-compose.yml включает:
PostgreSQL (база данных star_db)
Zookeeper (координация Kafka)
Kafka (брокер сообщений, топик input-topic)
Flink (JobManager + TaskManager)
Data Producer (Python скрипт для отправки данных в Kafka)

### 3. Код Apache Flink
SaleEvent.java - класс с полями: transaction_id, product_id, product_name, category, customer_id, customer_name, city, store_id, sale_amount, sale_timestamp
FlinkStarTransformer.java - потоковая обработка:
Чтение из Kafka (input-topic)
Десериализация JSON
Запись в PostgreSQL (fact_sales)

###  4. Инструкция по запуску

```bash
docker-compose up --build
Проверка работы producer

В логах должно появиться:

text
data-producer |  Done! Sent 10000 messages to Kafka topic 'input-topic'

Запуск Flink задачи 

bash
docker exec -it flink-jobmanager bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
export PATH=$JAVA_HOME/bin:$PATH
cd /tmp/flink-job
mvn clean package
flink run -c com.flink.FlinkStarTransformer target/flink-star-transformer-1.0.jar
Шаг 4: Проверка результата в PostgreSQL

bash
docker exec -it postgres_db psql -U flink_user -d star_db -c "SELECT COUNT(*) FROM fact_sales;"
```
Результаты выполнения

Скриншот 1: Общее количество записей (10 000)

screenshots/screenshot1_count.jpg

Скриншот 2: Пример данных из fact_sales

screenshots/screenshot2_data.jpg

Скриншот 3: Статистика по категориям

screenshots/screenshot3_categories.jpg

Скриншот 4: Топ-10 самых дорогих покупок

screenshots/screenshot4_top10.jpg

Анализ полученных данных

Категория	Количество продаж	Общая выручка
Clothing	2 036	1 045 724.10
Electronics	1 955	1 014 141.24
Books	2 001	1 004 889.24
Food	2 019	998 428.82
Sports	1 989	995 770.61
Выводы:

Все 10 000 записей успешно обработаны
Flink корректно прочитал данные из Kafka
Данные преобразованы в модель "звезда" и записаны в PostgreSQL
Наибольшая выручка у категории "Clothing"

Заключение

В ходе выполнения лабораторной работы было реализовано:

Читает 10 CSV файлов (10 000 строк) и отправляет их в Kafka
Flink streaming задача, которая читает из Kafka, преобразует данные и сохраняет в PostgreSQL
Docker Compose конфигурация для запуска всех сервисов
Полная инструкция по запуску и проверке работы
Все 10 000 записей успешно обработаны и загружены в базу данных.
Проверка результатов

bash
docker exec -it postgres_db psql -U flink_user -d star_db -c "SELECT COUNT(*) FROM fact_sales;"
# Ожидаемый результат: 10000


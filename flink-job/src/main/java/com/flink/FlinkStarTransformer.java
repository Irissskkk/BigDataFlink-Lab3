package com.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class FlinkStarTransformer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        // 1. Kafka источник (читаем из input-topic)
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers("kafka:9092")
            .setTopics("input-topic")
            .setGroupId("flink-consumer-group")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();
        
        DataStream<String> jsonStream = env.fromSource(
            source,
            WatermarkStrategy.noWatermarks(),
            "Kafka Source"
        );
        
        // 2. Парсим JSON в SaleEvent
        DataStream<SaleEvent> saleStream = jsonStream.map(json -> {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(json, SaleEvent.class);
        }).name("JSON Parser");
        
        // 3. Выводим в логи для отладки (первые 100 записей)
        saleStream.print();
        
        // 4. Сохраняем в PostgreSQL
        saleStream.addSink(JdbcSink.sink(
            "INSERT INTO fact_sales (transaction_id, product_id, product_name, category, " +
            "customer_id, customer_name, city, store_id, sale_amount, sale_timestamp) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (statement, event) -> {
                statement.setString(1, event.transactionId);
                statement.setInt(2, event.productId);
                statement.setString(3, event.productName);
                statement.setString(4, event.category);
                statement.setInt(5, event.customerId);
                statement.setString(6, event.customerName);
                statement.setString(7, event.city);
                statement.setInt(8, event.storeId);
                statement.setDouble(9, event.saleAmount);
            
                LocalDateTime dateTime = LocalDateTime.parse(
                    event.saleTimestamp, 
                    DateTimeFormatter.ISO_LOCAL_DATE_TIME
                );
                statement.setTimestamp(10, java.sql.Timestamp.valueOf(dateTime));
            },
            JdbcExecutionOptions.builder()
                .withBatchSize(100)
                .withBatchIntervalMs(200)
                .build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:postgresql://postgres:5432/star_db")
                .withDriverName("org.postgresql.Driver")
                .withUsername("flink_user")
                .withPassword("flink_pass")
                .build()
        )).name("PostgreSQL Sink");
        
        
        env.execute("Flink Star Schema Transformer");
    }
}
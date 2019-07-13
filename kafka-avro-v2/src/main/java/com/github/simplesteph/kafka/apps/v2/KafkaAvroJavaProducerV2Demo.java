package com.github.simplesteph.kafka.apps.v2;

import com.example.Customer;
import com.example.CustomerKey;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class KafkaAvroJavaProducerV2Demo {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
        // normal producer
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("acks", "all");
        properties.setProperty("retries", "10");
        // avro part
        properties.setProperty("key.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");



        Producer<CustomerKey, Customer> producer = new KafkaProducer<>(properties);

        String topic = "customer-avro";

        Random random = new Random();

        // copied from avro examples
        Customer customer = Customer.newBuilder()
                .setAge(random.nextInt(60))
                .setFirstName("John")
                .setLastName("Doe")
                .setHeight(178f)
                .setWeight(75f)
                .setEmail("john.doe@gmail.com")
                .setPhoneNumber("(123)-456-7890")
                .build();

        ProducerRecord<CustomerKey, Customer> producerRecord =
                new ProducerRecord<>(topic, new CustomerKey(UUID.randomUUID().toString()), customer);

        System.out.println(customer);
        producer.send(producerRecord, (metadata, exception) -> {
            if (exception == null) {
                System.out.println(metadata);
            } else {
                exception.printStackTrace();
            }
        });

        producer.flush();
        producer.close();

    }
}

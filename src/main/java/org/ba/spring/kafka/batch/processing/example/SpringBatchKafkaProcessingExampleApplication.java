package org.ba.spring.kafka.batch.processing.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.BatchLoggingErrorHandler;
import org.springframework.messaging.handler.annotation.Payload;

import java.util.HashMap;
import java.util.List;

@SpringBootApplication
public class SpringBatchKafkaProcessingExampleApplication {
    public static void main(String[] args) {
        SpringApplication.run(SpringBatchKafkaProcessingExampleApplication.class, args);
    }

    @Bean
    public DefaultKafkaConsumerFactory consumerFactory(){
        var config = new HashMap<>();

        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "group_one_v12_b12");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300);



        return new DefaultKafkaConsumerFactory(config, new StringDeserializer(),  new StringDeserializer());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory kafkaListenerContainerFactory(){
        var factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setBatchListener(true);


        factory.setBatchErrorHandler(new BatchLoggingErrorHandler());
        return factory;
    }


    @KafkaListener(
            topics = "test_topic",
            groupId = "group_one_v12_b12",
            containerFactory = "kafkaListenerContainerFactory",
            autoStartup = "true")
    public void consumeFromCoreTopicPartitionZERO(@Payload List<Object> containers){
        System.out.printf("consuming : %s\n", containers.size());
    }

}

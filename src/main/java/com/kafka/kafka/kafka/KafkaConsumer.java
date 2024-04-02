package com.kafka.kafka.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {

    private static Logger LOGGER = LoggerFactory.getLogger(KafkaConsumer.class);

    @KafkaListener(topics = "${spring.kafka.topic.name}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumer(String message) {
        try {
            LOGGER.info(String.format("Message received -> %s", message));
            // Process the message here
        } catch (MessageConversionException ex) {
            LOGGER.error("Error occurred during message conversion: " + ex.getMessage(), ex);
            // Handle the exception here, e.g., log the error, skip the message, etc.
        } catch (Exception e) {
            LOGGER.error("An unexpected error occurred: " + e.getMessage(), e);
            // Handle other exceptions here
        }
    }
}

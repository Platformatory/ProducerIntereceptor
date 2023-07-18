package com.platformatory.vivek;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

public class SourceTimeProducerInterceptor implements ProducerInterceptor<String, String> {

    @Override
    public void configure(Map<String, ?> configs) {
        // This method allows you to retrieve the configuration of the interceptor from the configuration properties
    }

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        // Generate a completely unique UUID for each record
        String uuid = UUID.randomUUID().toString();

        // Add the UUID to the headers of the record
        record.headers().add("UUID", uuid.getBytes(StandardCharsets.UTF_8));

        long currentTimeMillis = System.currentTimeMillis();
        String sourceTimeAsValue = String.valueOf(currentTimeMillis);

        return new ProducerRecord<>("RecordLatency", uuid, sourceTimeAsValue);
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        // This method is called when the broker acknowledges the receipt of the record
    }

    @Override
    public void close() {
        // This method is called when the interceptor is being shut down
    }
}

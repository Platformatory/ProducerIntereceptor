package com.platformatory.vivek;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class Interceptor implements ProducerInterceptor<String, String> {

    private SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");

    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        // Create the hash of record key and value
        String recordHash = createHash(producerRecord.key() + producerRecord.value());

        // Create a new record with the hash as the key and the current time as the value
        // and send it to the 'test_time' topic
        return new ProducerRecord<String, String>("test_time", producerRecord.partition(), producerRecord.timestamp(),
                recordHash, sdf.format(new Date()), producerRecord.headers());
    }

    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        // This is called when the record sent to the server has been acknowledged
    }

    public void close() {
        // THis is called when interceptor is closed
    }

    public void configure(Map<String, ?> map) {
        // This is called when configs are passed during initialization
    }

    private String createHash(String str) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] array = md.digest(str.getBytes());
            StringBuilder sb = new StringBuilder();
            for (byte b : array){
                sb.append(Integer.toHexString((b & 0xFF) | 0x100), 1, 3);
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Error Creating Hash" + e);
        }
    }
}

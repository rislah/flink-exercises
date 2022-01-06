package org.rislah.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.rislah.common.PageView;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final ObjectMapper objectMapper = new ObjectMapper();

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9094");
        props.put("acks", "all");

        KafkaProducer<String, PageView> producer = new KafkaProducer<String, PageView>(props,
                new StringSerializer(),
                new PageViewSerializer());

        for (int i = 0; i < 10000; i++) {
            PageView pageView = new PageView("android", String.valueOf(i));
            producer.send(new ProducerRecord<String, PageView>(
                            "page.view",
                            pageView.getUserId(),
                            pageView
                    ))
                    .get();
        }

        System.out.println("Hello World!");
    }
}

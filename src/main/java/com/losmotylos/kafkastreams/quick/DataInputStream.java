package com.losmotylos.kafkastreams.quick;

import kafka.utils.MockTime;
import kafka.zk.EmbeddedZookeeper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.integration.utils.KafkaEmbedded;

import java.io.IOException;
import java.util.Properties;

/**
 * @author kaczmat on 1/26/18.
 */
public class DataInputStream {
    public static final String LOCALHOST_9092 = "localhost:9092";
    public static final String TOPIC_IN = "streams-plaintext-input";

    public static void main(String[] args) throws IOException {
        EmbeddedZookeeper zookeeperServer = new EmbeddedZookeeper();

        Properties kafkaServerProperties = new Properties();
        kafkaServerProperties.put("brokerid", "1");
        kafkaServerProperties.put("port", "9092");
        kafkaServerProperties.put("auto.create.topics.enable", true);
        kafkaServerProperties.put("offsets.topic.num.partitions", "1");
        kafkaServerProperties.put("zookeeper.connect", "localhost:" + zookeeperServer.port());
        kafkaServerProperties.put("offsets.topic.replication.factor", "1");
        kafkaServerProperties.put("transaction.state.log.replication.factor", "1");
        KafkaEmbedded kafkaServer = new KafkaEmbedded(kafkaServerProperties, new MockTime());

        Thread.setDefaultUncaughtExceptionHandler((t, e) -> e.printStackTrace());
        final KafkaProducer<String, String> producer = producer();
        new Thread(() -> {
            for (int i = 0; i < Integer.MAX_VALUE; i++) {
                int key = i % 10;
                String message = "value " + key;
                producer.send(new ProducerRecord<>(TOPIC_IN, String.valueOf(key), message));
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                kafkaServer.stop();
                zookeeperServer.shutdown();
            }
        });

    }

    private static KafkaProducer<String, String> producer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, LOCALHOST_9092);
//        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "AAA");
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        return producer;
    }
}

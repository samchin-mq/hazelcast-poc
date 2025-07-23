package org.example.hazelcast;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.kafka.KafkaSinks;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;

import java.util.Properties;

public class KafkaStreamingJob {
    public static void main(String[] args) {
        // Create Hazelcast instance
        HazelcastInstance hz = Hazelcast.bootstrappedInstance();

        // Create the pipeline
        Pipeline pipeline = Pipeline.create();

        // Kafka properties for source
        Properties sourceProps = new Properties();
        sourceProps.setProperty("bootstrap.servers", "boot-9cf.exp2.5e7k1c.c4.kafka.ap-northeast-1.amazonaws.com:9092,boot-93g.exp2.5e7k1c.c4.kafka.ap-northeast-1.amazonaws.com:9092,boot-rzz.exp2.5e7k1c.c4.kafka.ap-northeast-1.amazonaws.com:9092");
        sourceProps.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        sourceProps.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        sourceProps.setProperty("auto.offset.reset", "earliest");
        sourceProps.setProperty("group.id", "hazelcast-group");
        sourceProps.setProperty("security.protocol", "PLAINTEXT");

        // Kafka properties for sink
        Properties sinkProps = new Properties();
        sinkProps.setProperty("bootstrap.servers", "boot-9cf.exp2.5e7k1c.c4.kafka.ap-northeast-1.amazonaws.com:9092,boot-93g.exp2.5e7k1c.c4.kafka.ap-northeast-1.amazonaws.com:9092,boot-rzz.exp2.5e7k1c.c4.kafka.ap-northeast-1.amazonaws.com:9092");
        sinkProps.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        sinkProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        sinkProps.setProperty("security.protocol", "PLAINTEXT");
        // Build the pipeline
        pipeline.readFrom(KafkaSources.<String, String>kafka(
                        sourceProps,
                        "sam-topic"
                ))
                .withoutTimestamps()
                .writeTo(KafkaSinks.kafka(
                        sinkProps,
                        "sam-post-topic"
                ));

        // Submit the job
        try {
            hz.getJet().newJob(pipeline);
            System.out.println("Kafka streaming job started successfully");
        } catch (Exception e) {
            System.err.println("Error submitting job: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
package com.example.akkastreamspractice;

import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class AkkaStreamsGraph {

    public static void main(String[] args) {

    }

    public static ConsumerSettings<String, String> getConsumerSettings(ActorSystem actorSystem) {
        return ConsumerSettings
                .create(actorSystem, new StringDeserializer(), new StringDeserializer())
                // The stage will delay stopping the internal actor to allow processing of messages already in the
                // stream (required for successful committing). This can be set to 0 for streams using DrainingControl
                .withStopTimeout(Duration.ofSeconds(0))
                .withBootstrapServers("10.0.1.212:9092")
                .withGroupId("test")
                .withClientId("111")
                .withProperties(defaultConsumerConfig());
    }

    public static Map<String, String> defaultConsumerConfig() {
        Map<String, String> defaultConsumerConfig = new HashMap<>();
        defaultConsumerConfig.put("auto.offset.reset", "earliest");
        defaultConsumerConfig.put("max.poll.interval.ms", "2147483647");
        defaultConsumerConfig.put("max.poll.records", "100");
        defaultConsumerConfig.put("max.partition.fetch.bytes", Integer.toString(5 * 1024 * 1024));
        defaultConsumerConfig.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RoundRobinAssignor");
        return defaultConsumerConfig;
    }
}

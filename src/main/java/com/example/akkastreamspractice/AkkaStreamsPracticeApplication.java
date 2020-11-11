package com.example.akkastreamspractice;

import akka.Done;
import akka.actor.ActorSystem;
import akka.kafka.CommitterSettings;
import akka.kafka.ConsumerMessage;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Committer;
import akka.kafka.javadsl.Consumer;
import akka.stream.ActorAttributes;
import akka.stream.Supervision;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class AkkaStreamsPracticeApplication {
    private static AtomicInteger count = new AtomicInteger(0);

    public static void main(String[] args) {
        akkaStreamsKafkaExample();
        //akkaStreamsExample();
    }

    private static void akkaStreamsKafkaExample() {
        ActorSystem actorSystem = ActorSystem.create();
        ConsumerSettings<String, String> consumerSettings = getConsumerSettings(actorSystem);
        CommitterSettings committerSettings = CommitterSettings.create(actorSystem)
                .withMaxBatch(20)
                .withMaxInterval(Duration.ofSeconds(30));
        int groupBy = 10;
        Consumer.committableSource(consumerSettings, Subscriptions.topics("taxilla-events"))
                .filter(msg -> msg.record().offset() % groupBy != 3)
                .groupBy(groupBy, msg -> msg.record().offset() % groupBy)
                .groupedWithin(Integer.MAX_VALUE, Duration.ofSeconds(10))
                .mapAsync(1, msgs -> {
                            return CompletableFuture.supplyAsync(() -> {
                                System.out.println(msgs.get(0).record().offset() % groupBy + " " + msgs.size() + " ");
                                        //msgs.stream().map(msg -> msg.record().offset()).collect(Collectors.toList()));
                                ConsumerMessage.CommittableOffset committableOffset = null;
                                for (ConsumerMessage.CommittableMessage<String, String> msg : msgs) {
                                    try {
                                        //.Thread.sleep(1);
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                    //System.out.println(Thread.currentThread() + " " + msg.record().offset() + " " + (msg.record().offset() % 100)
                                    //        + " " + msg.record().partition() + " size " + msgs.size());
                                    committableOffset = msg.committableOffset();
                                }
                                return committableOffset;
                            }, actorSystem.dispatcher()).thenApply(param -> msgs.get(msgs.size() - 1).committableOffset());
                        }
                )
                .mergeSubstreams()
                .toMat(Committer.sink(committerSettings), Keep.both())
                .withAttributes(ActorAttributes.supervisionStrategy(
                        ex -> Supervision.restart()))
                .mapMaterializedValue(Consumer::createDrainingControl)
                .run(actorSystem);
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
        defaultConsumerConfig.put("max.poll.records", "2000");
        defaultConsumerConfig.put("max.partition.fetch.bytes", Integer.toString(5 * 1024 * 1024));
        defaultConsumerConfig.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RoundRobinAssignor");
        return defaultConsumerConfig;
    }

    private static void akkaStreamsExample() {
        List<Pair> collect = IntStream.range(1, 1000).mapToObj(i -> new Pair("pair" + (i / 10), "pair" + i)).collect(Collectors.toList());
        Source.from(collect)
                .log("Incoming pair ")
                .groupBy(100, elem -> elem.key)
                .mapAsync(1, msg -> {
                    return CompletableFuture.supplyAsync(() -> {
                        //int millis = new Random().nextInt(5000);
                        try {
                            Thread.sleep(5000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        System.out.println(Thread.currentThread() + " " + msg + " " + 5000);
                        return CompletableFuture.completedStage(msg);
                    });
                })
                .mergeSubstreams()
                .to(Sink.ignore())
                .run(ActorSystem.create());
    }

    public static class Pair {
        private String key;
        private String value;

        public Pair(String key, String value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String toString() {
            return "{" +
                    "key='" + key + '\'' +
                    ", value='" + value + '\'' +
                    '}';
        }
    }
}

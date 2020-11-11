package com.example.akkastreamspractice;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.kafka.CommitterSettings;
import akka.kafka.ConsumerMessage;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Committer;
import akka.kafka.javadsl.Consumer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class AkkaStreamsPartitionedSource {

    public static void main(String[] args) {
        ActorSystem actorSystem = ActorSystem.create();
        ConsumerSettings<String, String> consumerSettings = getConsumerSettings(actorSystem);
        CommitterSettings committerSettings = CommitterSettings.create(actorSystem)
                .withMaxBatch(100)
                .withMaxInterval(Duration.ofSeconds(30));
        Instant startInstant = Instant.now();
        AtomicInteger counter = new AtomicInteger(0);
        Consumer.committablePartitionedSource(consumerSettings, Subscriptions.topics("taxilla-events"))
                .mapAsyncUnordered(24, pair -> {
                    Source<ConsumerMessage.CommittableMessage<String, String>, NotUsed> source = pair.second();
                    return source
                            .groupBy(Integer.MAX_VALUE, msg -> msg.record().offset() % 10)
                            .mapAsync(1, msg -> CompletableFuture.supplyAsync(() -> {
                                        try {
                                            Thread.sleep(100);
                                        } catch(Exception ex) {}
                                        counter.addAndGet(1);
                                        System.out.println("group " + msg.record().offset() % 10 + " offset : " + msg.record().offset() +
                                                " partition : " + msg.record().partition() + " " + counter.get()  +  " " + Duration.between(startInstant, Instant.now()).getSeconds());
                                        return msg.record().offset();
                                    }).thenApply(resp -> msg.committableOffset())
                            )
                            .mergeSubstreams()
                            .grouped(100)
                            .map(offsets -> offsets.stream().sorted(Comparator.comparingLong(offset -> offset.partitionOffset()._2())).collect(Collectors.toList()))
                            .mapConcat(offsets -> offsets)
                            .runWith(Committer.sink(committerSettings), actorSystem);
                })
                .toMat(Sink.ignore(), Consumer::createDrainingControl)
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
        defaultConsumerConfig.put("max.poll.records", "100");
        defaultConsumerConfig.put("max.partition.fetch.bytes", Integer.toString(5 * 1024 * 1024));
        defaultConsumerConfig.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RoundRobinAssignor");
        return defaultConsumerConfig;
    }
}

package com.example.akkastreamspractice;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.function.Function;
import akka.kafka.CommitterSettings;
import akka.kafka.ConsumerMessage;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Committer;
import akka.kafka.javadsl.Consumer;
import akka.stream.*;
import akka.stream.javadsl.*;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class AkkaStreamsGraphPartitionedSource {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("akka.loglevel", "DEBUG");
        properties.setProperty("akka.stdout-loglevel", "DEBUG");
        properties.setProperty("akka.log-dead-letters", "10");
        properties.setProperty("akka.log-dead-letters-during-shutdown", "on");
        properties.setProperty("akka.loggers.0", "akka.event.slf4j.Slf4jLogger");
        properties.setProperty("akka.logging-filter", "akka.event.slf4j.Slf4jLoggingFilter");
        properties.setProperty("akka.actor.provider", "akka.actor.LocalActorRefProvider");
        properties.setProperty("akka.jvm-exit-on-fatal-error", "on");

        ActorSystem actorSystem = ActorSystem.create("TestAkkaSystem", ConfigFactory.parseProperties(properties));
        ConsumerSettings<String, String> consumerSettings = getConsumerSettings(actorSystem);
        CommitterSettings committerSettings = CommitterSettings.create(actorSystem)
                .withMaxBatch(100)
                .withMaxInterval(Duration.ofSeconds(30));

        Consumer.committablePartitionedSource(consumerSettings, Subscriptions.topics("taxilla-events"))
                .mapAsyncUnordered(40, pair -> {
                    Source<ConsumerMessage.CommittableMessage<String, String>, NotUsed> source = pair.second();
                    Graph<FlowShape<ConsumerMessage.CommittableMessage<String, String>, ConsumerMessage.CommittableOffset>, NotUsed> flowShapeNotUsedGraph = prepareGraph();
                    return source.via(flowShapeNotUsedGraph)
                            .runWith(Committer.sink(committerSettings), actorSystem);
                })
                .toMat(Sink.ignore(), Consumer::createDrainingControl)
                .run(actorSystem);
    }

    private static Graph<FlowShape<ConsumerMessage.CommittableMessage<String, String>, ConsumerMessage.CommittableOffset>, NotUsed> prepareGraph() {
        Graph<UniformFanOutShape<ConsumerMessage.CommittableMessage<String, String>, ConsumerMessage.CommittableMessage<String, String>>, NotUsed>
                clusterPartitioning = fanOutBasedOnCluster();
        Flow<ConsumerMessage.CommittableMessage, ConsumerMessage.CommittableOffset, NotUsed> partition1OutFlow = flowOfMessagesToBeIgnored();
        Flow<ConsumerMessage.CommittableMessage, ConsumerMessage.CommittableOffset, NotUsed> partition2OutFlow = flowOfMessagesToBeProcessed();

        return GraphDSL.create(builder -> {
            UniformFanOutShape<ConsumerMessage.CommittableMessage<String, String>, ConsumerMessage.CommittableMessage<String, String>>
                    clusterPartitioningShape = builder.add(clusterPartitioning);
            UniformFanInShape<ConsumerMessage.CommittableOffset, ConsumerMessage.CommittableOffset> fanInShape = builder.add(Merge.create(2));

            builder.from(clusterPartitioningShape.out(0))
                        .via(builder.add(partition1OutFlow))
                        .toInlet(fanInShape.in(0))
                    .from(clusterPartitioningShape.out(2))
                        .via(builder.add(partition2OutFlow))
                        .toInlet(fanInShape.in(1));

            Flow<ConsumerMessage.CommittableOffset, ConsumerMessage.CommittableOffset, NotUsed> orderOffsetsFlow =
                    Flow.fromFunction((ConsumerMessage.CommittableOffset offset) -> offset)
                            .grouped(100)
                        .map(offsets ->
                                offsets.stream()
                                        .sorted(Comparator.comparingLong(offset -> offset.partitionOffset()._2()))
                                        .collect(Collectors.toList())
                        )
                        .mapConcat(offsets -> offsets);
            FlowShape<ConsumerMessage.CommittableOffset, ConsumerMessage.CommittableOffset> orderOffsetsFlowShape = builder.add(orderOffsetsFlow);
            builder.from(fanInShape.out()).toInlet(orderOffsetsFlowShape.in());

            return FlowShape.of(clusterPartitioningShape.in(), orderOffsetsFlowShape.out());
        });
    }

    private static Graph<UniformFanOutShape<ConsumerMessage.CommittableMessage<String, String>, ConsumerMessage.CommittableMessage<String, String>>, NotUsed> fanOutBasedOnCluster() {
        Graph<UniformFanOutShape<ConsumerMessage.CommittableMessage<String, String>, ConsumerMessage.CommittableMessage<String, String>>, NotUsed> clusterPartitioning
                = Partition.create(2, (Function<ConsumerMessage.CommittableMessage<String, String>, Integer>) tuple -> {
            if (tuple.record().value().contains("ewb")) {
                return 0;
            } else {
                return 1;
            }
        });
        return clusterPartitioning;
    }

    private static Flow<ConsumerMessage.CommittableMessage, ConsumerMessage.CommittableOffset, NotUsed> flowOfMessagesToBeProcessed() {
        Instant startInstant = Instant.now();
        AtomicInteger regCounter = new AtomicInteger(0);
        Flow<ConsumerMessage.CommittableMessage, ConsumerMessage.CommittableOffset, NotUsed> partition2OutFlow = Flow.of(ConsumerMessage.CommittableMessage.class)
                .log("flowOfMessagesToBeProcessed")
                .withAttributes(Attributes.createLogLevels(Attributes.logLevelDebug()))
                .groupBy(Integer.MAX_VALUE, msg -> msg.record().offset() % 10)
                .mapAsync(1, msg -> CompletableFuture.supplyAsync(() -> {
                            try {
                                Thread.sleep(100);
                            } catch (Exception ex) {
                            }
                            regCounter.addAndGet(1);
                            System.out.println("group " + msg.record().offset() % 10 + " offset : " + msg.record().offset() +
                                    " partition : " + msg.record().partition() + " " + regCounter.get() + " " + Duration.between(startInstant, Instant.now()).getSeconds());
                            return msg.record().offset();
                        }).thenApply(resp -> msg.committableOffset())
                )
                .mergeSubstreams();
        return partition2OutFlow;
    }

    private static Flow<ConsumerMessage.CommittableMessage, ConsumerMessage.CommittableOffset, NotUsed> flowOfMessagesToBeIgnored() {
        Instant instant = Instant.now();
        AtomicInteger ewbCounter = new AtomicInteger(0);
        Flow<ConsumerMessage.CommittableMessage, ConsumerMessage.CommittableOffset, NotUsed> partition1OutFlow = Flow.of(ConsumerMessage.CommittableMessage.class)
                .log("flowOfMessagesTobeIgnored")
                .withAttributes(Attributes.createLogLevels(Attributes.logLevelDebug()))
                .map(msg -> {
            ewbCounter.addAndGet(1);
            System.out.println("group " + msg.record().offset() % 10 + " offset : " + msg.record().offset() +
                    " partition : " + msg.record().partition() + " " + ewbCounter.get() + " " + Duration.between(instant, Instant.now()).getSeconds());
            return msg.committableOffset();
        });
        return partition1OutFlow;
    }

    public static ConsumerSettings<String, String> getConsumerSettings(ActorSystem actorSystem) {
        return ConsumerSettings
                .create(actorSystem, new StringDeserializer(), new StringDeserializer())
                // The stage will delay stopping the internal actor to allow processing of messages already in the
                // stream (required for successful committing). This can be set to 0 for streams using DrainingControl
                .withStopTimeout(Duration.ofSeconds(0))
                .withBootstrapServers("10.0.1.207:9092")
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

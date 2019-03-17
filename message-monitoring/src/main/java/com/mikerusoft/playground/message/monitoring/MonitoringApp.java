package com.mikerusoft.playground.message.monitoring;

import com.mikerusoft.playground.kafkastreamsinit.JSONSerde;
import com.mikerusoft.playground.kafkastreamsinit.JsonTimestampExtractor;
import com.mikerusoft.playground.kafkastreamsinit.KafkaStreamUtils;
import com.mikerusoft.playground.models.monitoring.MessageMonitor;
import com.mikerusoft.playground.models.monitoring.MessageStatus;
import com.mikerusoft.playground.models.monitoring.ReceivedMessage;
import com.mikerusoft.playground.models.monitoring.SentMessage;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.mikerusoft.playground.kafkastreamsinit.KafkaStreamUtils.createProduced;

@SpringBootApplication
public class MonitoringApp implements CommandLineRunner {
    public static void main(String[] args) {
        SpringApplication.run(MonitoringApp.class, args);
    }

    @Value("${broker_url:localhost:9092}")
    private String url;

    @Override
    public void run(String... args) throws Exception {

        final StreamsBuilder builder = new StreamsBuilder();

        Properties config = KafkaStreamUtils.streamProperties("monitoring-app", url, ReceivedMessage.class);

        // creating KTable from received-messages
        KTable<String, MessageMonitor> msgMonitorKtable = builder.stream("received-messages",
                Consumed.with(Serdes.String(), new JSONSerde<>(ReceivedMessage.class))
                .withTimestampExtractor(new JsonTimestampExtractor<>(ReceivedMessage.class, ReceivedMessage::getReceivedTime))
            )
            .peek((k,v) -> System.out.println(k))
            .groupByKey()
            // since there is no API to transform stream into KTable, let's do "reduce" instead to implements some redundant Processor
            .reduce((v1, v2) -> v1)
            .mapValues(v -> MessageMonitor.builder()
                        .receivedTime(v.getReceivedTime())
                        .currentTime(System.currentTimeMillis())
                        .id(v.getId())
                    .build(),
                Materialized.with(Serdes.String(), new JSONSerde<>(MessageMonitor.class))
            );
        String receivedMessageStateStoreName = msgMonitorKtable.queryableStoreName();
        // let's put stream into "message-monitoring" to recognize when message is received but never sent (and received DR)
        msgMonitorKtable.toStream()
            .to("message-monitoring", createProduced(MessageMonitor.class));

        // when message is sent, let's join it with originally received message (stored in KTable),
        // convert to MessageMonitor object and put it into "message-monitoring" topic
        builder.stream("sent-messages",
            Consumed.with(Serdes.String(), new JSONSerde<>(SentMessage.class))
            .withTimestampExtractor(new JsonTimestampExtractor<>(SentMessage.class, SentMessage::getSentTime))
        ).join(msgMonitorKtable, (sent, monitor) -> monitor.toBuilder()
                .id(sent.getId())
                .extMessageId(sent.getExtMessageId())
                .providerId(sent.getProviderId())
                .sentStatusTime(sent.getSentTime())
                .sentStatus(sent.getStatus())
                .build().increaseCounter(1),
            Joined.otherValueSerde(new JSONSerde<>(MessageMonitor.class))
        ).to("message-monitoring", createProduced(MessageMonitor.class));

        // when we receive DR, let's join it with originally received message (stored in KTable),
        // convert to MessageMonitor object and put it into "message-monitoring" topic
        builder.stream("status-messages",
            Consumed.with(Serdes.String(), new JSONSerde<>(MessageStatus.class))
            .withTimestampExtractor(new JsonTimestampExtractor<>(MessageStatus.class, MessageStatus::getStatusTime))
        ).join(msgMonitorKtable, (s, monitor) -> monitor.toBuilder()
                .id(s.getId())
                .extMessageId(s.getExtMessageId())
                .providerId(s.getProviderId())
                .drStatus(s.getStatus())
                .drStatusTime(s.getStatusTime())
            .build().increaseCounter(1)
        ).to("message-monitoring", Produced.with(Serdes.String(), new JSONSerde<>(MessageMonitor.class)));

        // now let's to examine "message-monitoring" topic: 1. aggregate all received data and with help of
        // of processor, remove messages which are OK (received, sent and have DR)
        KTable<String, MessageMonitor> aggregate = builder.stream("message-monitoring",
            Consumed.with(Serdes.String(), new JSONSerde<>(MessageMonitor.class))
            .withTimestampExtractor(new JsonTimestampExtractor<>(MessageMonitor.class, MessageMonitor::getSentTime))
        ).groupByKey()
        .aggregate(MessageMonitor::new, (k, v, a) -> a.merge(v))
        ;

        String storeName = aggregate.queryableStoreName();

        aggregate.toStream().process(new ProcessorSupplier<String, MessageMonitor>() {
            @Override
            public Processor<String, MessageMonitor> get() {
                return new Processor<String, MessageMonitor>() {

                    private KeyValueStore<String, MessageMonitor> monitorStore;
                    private KeyValueStore<String, MessageMonitor> receivedStore;
                    private ProcessorContext context;

                    @Override
                    public void init(ProcessorContext context) {
                        this.context = context;
                        this.monitorStore = (KeyValueStore<String, MessageMonitor>) context.getStateStore(storeName);
                        this.receivedStore = (KeyValueStore<String, MessageMonitor>) context.getStateStore(receivedMessageStateStoreName);
                    }

                    @Override
                    public void process(String key, MessageMonitor value) {
                        // we should reach this processor after aggregation
                        if (value.getCounter() == 2) { // means we both sent message and received DR
                            // so let's delete it
                            this.monitorStore.delete(key);
                            this.receivedStore.delete(key);
                            // TODO: do I need to store context and perform "commit" ?
                            this.context.commit();
                        }
                    }

                    @Override
                    public void close() {}
                };
            }
        });

        Topology topology = builder.build();

        topology = topology.addSource("source", ".*message-monitoring.*")
        // TODO: could I use only one processor
        .addProcessor("send-alert", new ProcessorSupplier<Object, Object>() {
            @Override
            public Processor<Object, Object> get() {
                return new Processor<Object, Object>() {

                    private KeyValueStore<String, MessageMonitor> monitorStore;
                    private KeyValueStore<String, MessageMonitor> receivedStore;

                    @Override
                    public void init(ProcessorContext context) {
                        this.monitorStore = (KeyValueStore<String, MessageMonitor>) context.getStateStore(storeName);
                        this.receivedStore = (KeyValueStore<String, MessageMonitor>) context.getStateStore(receivedMessageStateStoreName);
                        context.schedule(100L, PunctuationType.WALL_CLOCK_TIME, time -> {
                            long now = System.currentTimeMillis();
                            KeyValueIterator<String, MessageMonitor> all = this.monitorStore.all();
                            List<KeyValue<String, MessageMonitor>> alertsToBeSent = new ArrayList<>();
                            while (all.hasNext()) {
                                KeyValue<String, MessageMonitor> next = all.next();
                                MessageMonitor messageMonitor = next.value;
                                if (waitTooLong(now, messageMonitor.getCurrentTime())) {
                                    // TODO: send alert
                                    alertsToBeSent.add(next);
                                }
                            }
                            alertsToBeSent.forEach(pair -> {
                                // forwards request to sink -> topic "message-alerts"
                                context.forward(pair.key, pair.value, To.child("alerts"));
                                // since alert has been sent, let's delete from key/value stores
                                this.monitorStore.delete(pair.key);
                                this.receivedStore.delete(pair.key);
                            });
                            context.commit();
                        });
                    }

                    private boolean waitTooLong(long now, long receivedAt) {
                        return receivedAt + TimeUnit.SECONDS.toMillis(120L) < now;
                    }

                    @Override
                    public void process(Object key, Object value) {}

                    @Override
                    public void close() {}
                };
            }
        }, "source")
        .addSink("alerts", "message-alerts",
                new StringSerializer(), new JSONSerde<>(MessageMonitor.class), "send-alert");
        System.out.println("" + topology.describe());

        KafkaStreamUtils.runStream(new KafkaStreams(topology, config));
    }
}

package com.mikerusoft.playground.udhi;

import com.mikerusoft.playground.kafkastreamsinit.JSONSerde;
import com.mikerusoft.playground.kafkastreamsinit.KafkaStreamUtils;
import com.mikerusoft.playground.models.udhi.GroupMessage;
import com.mikerusoft.playground.models.udhi.ReadyMessage;
import com.mikerusoft.playground.models.udhi.UdhiMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@SpringBootApplication
@Slf4j
public class UdhiNoWindowWithProcessorApp implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(UdhiNoWindowWithProcessorApp.class, args);
    }

    @Value("${broker_url:localhost:9092}")
    private String url;

    @Override
    public void run(String... args) throws Exception {

        String appId = "udhi-app" + UUID.randomUUID().toString();
        Properties config = KafkaStreamUtils.streamProperties(appId, url, UdhiMessage.class);

        final StreamsBuilder builder = new StreamsBuilder();


        KeyValueBytesStoreSupplier supplier = Stores.persistentKeyValueStore("waiting-for-last-udhi");

        KafkaStreamUtils.createStringJsonStream(UdhiMessage.class, "received-messages", builder)
            .groupByKey()
            .aggregate(
                GroupMessage::new,
                (key, message, group) -> addMessageToGroup(message, group),
                defineAggregateStore(supplier)
            )
            .filter( (key, group) -> group.ready())
            .toStream()
            .filter((key,group) -> group != null)
            .map( (key, group) -> new KeyValue<>(key, group.convert()))
            .to("ready-messages", Produced.with(Serdes.String(), new JSONSerde<>(ReadyMessage.class)));

        Topology topology = builder.build();

        topology =
                topology.addSource("source", Pattern.compile(".*waiting-for-last-udhi.*"))
                .addProcessor("pr", ExpirationProcessor::new, "source")
                .connectProcessorAndStateStores("pr", "waiting-for-last-udhi");

        System.out.println("" + topology.describe());

        KafkaStreams kafkaStreams = new KafkaStreams(topology, config);

        KafkaStreamUtils.runStream(kafkaStreams);
    }

    private static GroupMessage addMessageToGroup(UdhiMessage v, GroupMessage a) {
        if (a.getSize() == 0) { a.setSize(v.getSize());}
        a.add(v);
        return a;
    }

    private static Materialized<String, GroupMessage, KeyValueStore<Bytes, byte[]>> defineAggregateStore(KeyValueBytesStoreSupplier supplier) {
        return Materialized.<String, GroupMessage>as(supplier)
            .withKeySerde(Serdes.String()).withValueSerde(new JSONSerde<>(GroupMessage.class)).withCachingDisabled();
    }

    private static final long WAIT_TIME_MS = TimeUnit.SECONDS.toMillis(60);

    public static class ExpirationProcessor implements Processor {

        private ProcessorContext context;
        private KeyValueStore<String, GroupMessage> kvStore;

        @Override
        @SuppressWarnings("unchecked")
        public void init(ProcessorContext context) {
            this.context = context;
            this.kvStore = (KeyValueStore<String, GroupMessage>) context.getStateStore("waiting-for-last-udhi");
            this.context.schedule(5000L, PunctuationType.WALL_CLOCK_TIME, (timestamp) -> {
                long current = System.currentTimeMillis();
                List<String> toBeResendAsSingles = new ArrayList<>();
                List<String> toBeRemoved = new ArrayList<>();
                KeyValueIterator<String, GroupMessage> all = this.kvStore.all();
                while (all.hasNext()) {
                    KeyValue<String, GroupMessage> keyValue = all.next();
                    String key = keyValue.key;
                    GroupMessage value = keyValue.value;
                    if (!value.ready() && isExpired(current, value)) {
                        toBeResendAsSingles.add(key);
                        log.info("Prepared to remove value with key {} and indexes {}",
                                key, joinIndexesToString(value));
                    } else if (value.ready() && isExpired(current, value)) {
                        toBeRemoved.add(key);
                    }
                }
                toBeResendAsSingles.forEach(key -> {
                    // now we have UhiMessages fo which we didn't receive the full toBeResendAsSingles
                    // so let's convert every udhi to single GroupMessage - send it again
                    // to context - to be converted into ReadyMessage
                    // we have some possible "race condition" if we suddenly receive some missing part
                    // of same GroupMessage, before we finish
                    GroupMessage groupMessage = this.kvStore.get(key);
                    List<GroupMessage> expand = groupMessage.expand();
                    expand.forEach(g -> this.context.forward(key, g, To.all()));
                    this.kvStore.delete(key);
                });
                // removed old records
                toBeRemoved.forEach(key -> this.kvStore.delete(key));
                this.context.commit();
            });
        }

        private boolean isExpired(long current, GroupMessage value) {
            return current - value.getTimeIngested() > WAIT_TIME_MS;
        }

        private static String joinIndexesToString(GroupMessage value) {
            return value.getParts().stream().map(UdhiMessage::getInd).map(String::valueOf).collect(Collectors.joining(","));
        }

        @Override
        public void process(Object key, Object value) {
            // nothing to do since this processor is called on any "waiting-for-last-udhi"
            // ingestion for aggregator topic, which is processed by stream's aggregate method, too
        }

        @Override
        public void close() {}
    }
}

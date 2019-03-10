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
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

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

        Properties config = KafkaStreamUtils.streamProperties("udhi-app" + UUID.randomUUID().toString(), url, UdhiMessage.class);

        final StreamsBuilder builder = new StreamsBuilder();


        KeyValueBytesStoreSupplier supplier = Stores.persistentKeyValueStore("waiting-for-last-udhi");

        //addStateStore(builder, supplier);

        KafkaStreamUtils.createStringJsonStream(UdhiMessage.class, "received-messages", builder)
            .groupByKey()
            .aggregate(
                GroupMessage::new,
                (k, v, a) -> {if (a.getSize() == 0) { a.setSize(v.getSize());} a.add(v); return a;},
                defineAggregateStore(supplier)
            )
            .filter( (k, v) -> v.ready())
            .toStream()
            .filter((k,v) -> v != null)
            .map( (k, v) -> new KeyValue<>(k, v.convert()))
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

    private static Materialized<String, GroupMessage, KeyValueStore<Bytes, byte[]>> defineAggregateStore(KeyValueBytesStoreSupplier supplier) {
        return Materialized.<String, GroupMessage>as(supplier)
        .withKeySerde(Serdes.String()).withValueSerde(new JSONSerde<>(GroupMessage.class)).withCachingDisabled();
    }

    public static class ExpirationProcessor implements Processor {

        private ProcessorContext context;
        private KeyValueStore<String, GroupMessage> kvStore;

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            this.kvStore= (KeyValueStore<String, GroupMessage>) context.getStateStore("waiting-for-last-udhi");
            this.context.schedule(1000L, PunctuationType.STREAM_TIME, (timestamp) -> {
                this.kvStore.all().forEachRemaining(keyValue -> {
                    String key = keyValue.key;
                    GroupMessage value = keyValue.value;
                    context.forward(key, value);
                });
            });
        }

        @Override
        public void process(Object key, Object value) {
            System.out.println(key);
        }

        @Override
        public void close() {

        }
    }
}

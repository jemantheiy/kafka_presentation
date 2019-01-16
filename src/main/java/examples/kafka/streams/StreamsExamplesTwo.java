package examples.kafka.streams;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;


import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueStore;


public class StreamsExamplesTwo {

    static final String INPUT_TOPIC = "jdbcjson-locations";
    static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    static final String KEYS_STORE = "jdbcjson-count-keys";
    static final String SALES_STORE = "jdbcjson-aggregate-sales";

    public static void main(String[] args) throws Exception {

        if (args.length > 2) {
            throw new IllegalArgumentException("usage: ... " +
                "[<bootstrap.servers> (optional, default: " + DEFAULT_BOOTSTRAP_SERVERS + ")] ");
            }

        final String bootstrapServers = args.length > 0 ? args[0] : DEFAULT_BOOTSTRAP_SERVERS;

        System.out.println("Connecting to Kafka cluster via bootstrap servers " + bootstrapServers);

        final StreamsBuilder builder = new StreamsBuilder();

        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "jdbcjson");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final KStream<Long, String> locationsJSON = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.Long(), Serdes.String()));
        locationsJSON.print(Printed.toSysOut());

        KStream<Long,Long> sales = locationsJSON.map((k, v) -> new KeyValue<Long, Long>(k, (Long)Long.parseLong(v)));

        // Count occurrences of each key
        KStream<Long, Long> countKeys = sales.groupByKey(Serialized.with(Serdes.Long(), Serdes.Long()))
            .count(Materialized.<Long, Long, KeyValueStore<Bytes, byte[]>>as(KEYS_STORE).withValueSerde(Serdes.Long()))
            .toStream();
        countKeys.print(Printed.toSysOut());

        // Aggregate values by key
        KStream<Long,Long> salesAgg = sales.groupByKey(Serialized.with(Serdes.Long(), Serdes.Long()))
            .reduce(
                (aggValue, newValue) -> aggValue + newValue)
            .toStream();
        
        salesAgg.print(Printed.toSysOut());

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

}
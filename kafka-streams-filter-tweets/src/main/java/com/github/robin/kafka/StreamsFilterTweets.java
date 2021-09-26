package com.github.robin.kafka;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class StreamsFilterTweets {

    public static Logger LOG = LoggerFactory.getLogger(StreamsFilterTweets.class);
    private static final String STREAM_FILTER_CONFIG = "stream_config.properties";

    public static void main(String[] args) throws IOException {

        Properties streamProps = getStreamProperties();
        String bootstrapServers = streamProps.getProperty("kafka.stream.bootstrap.server");
        String kafkaStreamId = streamProps.getProperty("kafka.stream.id");
        String incomingTopic = streamProps.getProperty("kafka.stream.incoming.topic");
        String filteredTopic = streamProps.getProperty("kafka.stream.filtered.topic");

        // create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, kafkaStreamId);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // input topic
        KStream<String, String> inputTopic = streamsBuilder.stream(incomingTopic);
        KStream<String, String> filteredStream = inputTopic.filter(
                // filter for tweets which has a user of over 10000 followers
                (k, jsonTweet) -> extractUserFollowersInTweet(jsonTweet) > 5
        );
        filteredStream.to(filteredTopic);

        // build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(
                streamsBuilder.build(),
                properties
        );

        // start our streams application
        kafkaStreams.start();
    }

    private static Integer extractUserFollowersInTweet(String tweetJson) {
        // gson library
        JsonElement userElement = new JsonParser().parse(tweetJson).getAsJsonObject().get("user");
        if(userElement != null && !userElement.isJsonNull()){
            JsonElement jsonElement = userElement.getAsJsonObject().get("followers_count");
            return jsonElement !=null ? jsonElement.getAsInt() : 0;
        }
        return 0;
    }

    private static Properties getStreamProperties() throws IOException {
        try (InputStream input = StreamsFilterTweets.class.getClassLoader().getResourceAsStream(STREAM_FILTER_CONFIG)) {
            Properties prop = new Properties();
            prop.load(input);
            return prop;
        } catch (IOException ex) {
            LOG.error("Unable to load producer properties " + ex.getMessage());
            throw new IOException(ex);
        }
    }
}

package twitter;


import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterHighThroughputProducer {

    private Logger logger = LoggerFactory.getLogger(TwitterHighThroughputProducer.class.getName());

    private String consumerKey = "dHyTuxpRLSwfFKMVV90n74dpv";
    private String consumerSecret = "tgL3BJDnQFfqAZ3Tgw0r6QLcGjgp0JjRywUVgeFcgn4XrjM0Dv";
    private String token = "1427267763022802946-k1BqqXuuWrLPLlSlsLl6DFFodJE3Yx";
    private String secret = "nKeLNP9LZSk8LddT8Ix5KMRKO6IbjO04O2J5CtFe0R7vn";
    //BEARER TOKEN AAAAAAAAAAAAAAAAAAAAACk7SwEAAAAAJnxKQP3w7TSTMuYqNni%2BB8mk%2Fc8%3DHxygVBsS1Dr45UQDVFvuplrIT2o2joBEBPhW4zLgDUJY5KxQ98

    List<String> terms = Lists.newArrayList("bitcoin", "usa", "politics"); // can add more than one term


    public TwitterHighThroughputProducer()
    {

    }


    public static void main(String[] args) {
        new TwitterHighThroughputProducer().run();
    }


    public void run(){

        logger.info("Setup");

        Integer blocking_queue_messages = 1000;
        Integer poll_timeout = 5;
        String topic = "twitter_tweets";

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(blocking_queue_messages);
        // BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(blocking_queue_messages);

        // Create a Twitter client
        Client client = createTwitterClient(msgQueue);
        // Attempts to establish a connection.
        client.connect();

        // Create a Kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        // Add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping application...");
            logger.info("Shutting down client from twitter...");
            client.stop();
            logger.info("Closing producer...");
            producer.close();
            logger.info("Done!");
        }));

        // Loop to send tweets to Kafka (or to the console) - Link Twitter client and Kafka producer
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(poll_timeout, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null)
            {
                logger.info(msg);
                producer.send(new ProducerRecord<>(topic, null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null)
                        {
                            logger.error("Something bad happened", e);
                        }
                    }
                });
            }
        }
        logger.info("End of application");
    }


    public Client createTwitterClient(BlockingQueue<String> msgQueue){

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        String hosebirdClientName = "Hosebird-Client-01";
        // Optional: set up some followings and track terms
        // List<Long> followings = Lists.newArrayList(1234L, 566788L);
        // hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name(hosebirdClientName)                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
                // .eventMessageQueue(eventQueue); optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }



    public KafkaProducer<String, String> createKafkaProducer(){

        // Create Producer properties
        Properties properties = new Properties();

        // Producer configurations
        try {
            properties.load(new FileReader("/home/priel/kafka-twitter-producer/src/main/java/twitter/producer.properties")); // Contains ssl configurations
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Adding producer configurations
        String bootstrapServers = "kafka-bootstrap.lab.test:443";
        String keySerializer = StringSerializer.class.getName();
        String valueSerializer = StringSerializer.class.getName();
        String idempotence = "true";
        String acks = "all";
        String retries = Integer.toString(Integer.MAX_VALUE);
        String maxInFlightRequestsPerConnection = "5";
        String compressionType = "snappy";
        String lingerMs = "20";
        String batchSize = Integer.toString(32 * 1024);


        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        // Kafka client will convert everything we send into bytes (0s and 1s)
        // Kafka has ready serializers

        // Create a safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, idempotence);
        properties.setProperty(ProducerConfig.ACKS_CONFIG, acks);
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, retries);
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInFlightRequestsPerConnection); // Kafka 2.0 >= 1.1 so we can keep this as 5. User 1 otherwise.


        // High throughput producer (at the expense of a bit of latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, batchSize); // 32 KB batch size


        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        // Key: String, Value: String
        return producer;
    }
}



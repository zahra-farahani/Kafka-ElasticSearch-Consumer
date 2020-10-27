package consumer;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ElasticSearchConsumer {

    private final static Logger LOGGER = LoggerFactory.getLogger(ElasticSearchConsumer.class);
    private final static JsonParser JSON_PARSER = new JsonParser();

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = createClient();

        KafkaConsumer<String, String> consumer = createConsumer("first_topic");

        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            BulkRequest bulkRequest = new BulkRequest(); //to enable batching

            int recordCount = records.count();
            LOGGER.info("Received " + recordCount + " records.");

            for(ConsumerRecord<String,String> record : records){

                IndexRequest request = new IndexRequest("first_topic_index");

                String id = record.topic() + "_" + record.partition() + "_" + record.offset(); //Kafka Generic Id
                request.id(id); //this is to make our consumer idempotent

                //insert data into ElasticSearch
                String jsonString = "{" +
                        "\"value\":\"" + record.value() + "\"" +
                        "}";
                request.source(jsonString, XContentType.JSON);

                bulkRequest.add(request);
                /*IndexResponse response = client.index(request, RequestOptions.DEFAULT);
                String responseId = response.getId();
                LOGGER.info(responseId);*/

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if(recordCount > 0) {
                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);

                LOGGER.info("Committing Offset...");
                consumer.commitSync();
                LOGGER.info("Offsets have been committed...");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        //client.close();
    }

    private static RestHighLevelClient createClient() {

        String hostName = "localhost";
        int hostPort = 9200;

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(hostName, hostPort, "http")));
        return client;
    }

    private static KafkaConsumer<String, String> createConsumer(String topic) {
        // Step1 : Create Consumer Properties
        // --> you can get a list of config options in :
        // https://kafka.apache.org/documentation/#consumerconfigs
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kafka-demo-elasticsearch");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");//means read from the beginning of topic --> "latest" means read just new messages
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); //disable auto commit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");

        // Step2 : Create consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        // Step3 : Subscribe consumer to our topic(s)
        kafkaConsumer.subscribe(Collections.singleton(topic));

        return kafkaConsumer;
    }

    private static String extractIdFromJSONMessage(String jsonMessage){
        return JSON_PARSER.parse(jsonMessage)
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }
}

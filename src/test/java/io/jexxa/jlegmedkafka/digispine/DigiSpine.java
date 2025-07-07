package io.jexxa.jlegmedkafka.digispine;

import io.jexxa.common.facade.logger.SLF4jLogger;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class DigiSpine {
    private final KafkaContainer kafkaBroker;
    private final GenericContainer<?> schemaRegistry;
    private final Properties kafkaProperties;


    public DigiSpine()
    {
        Network network = Network.newNetwork();
        kafkaBroker = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:8.0.0"))
                .withNetwork(network)
                .withNetworkAliases("kafka")
                .withKraft();
        kafkaBroker.start();

        schemaRegistry = new GenericContainer<>(
                DockerImageName.parse("confluentinc/cp-schema-registry:8.0.0"))
                .withNetwork(network)
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9092")
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                .withExposedPorts(8081)
                .waitingFor(Wait.forHttp("/subjects"));
        schemaRegistry.start();

        var schemaRegistryUrl = "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081);
        kafkaProperties = new Properties();
        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker.getBootstrapServers());
        kafkaProperties.put("schema.registry.url", schemaRegistryUrl);
    }

    /**
     * Creates a new Properties object containing all base configuration for kafka and schema registry
     */
    public Properties kafkaProperties()
    {
        Properties properties = new Properties();
        properties.putAll(kafkaProperties);
        return properties;
    }

    public void reset()
    {
        deleteTopics();
    }

    public void stop()
    {
        schemaRegistry.stop();
        kafkaBroker.stop();
    }

    @SuppressWarnings("unused")
    public void createTopic(String topic){
        try (AdminClient admin = AdminClient.create(Collections.singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker.getBootstrapServers()))) {
            if( ! admin.listTopics().names().get().contains(topic ) )
            {
                admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1))).all().get();
            } else {
                SLF4jLogger.getLogger(DigiSpine.class).warn("Topic {} already exist", topic);
            }
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    @SuppressWarnings("unused")
    public void deleteTopic(String topic){
        try (AdminClient admin = AdminClient.create(Collections.singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker.getBootstrapServers()))) {
            if( admin.listTopics().names().get().contains(topic ) )
            {
                admin.deleteTopics(Collections.singletonList(topic));
            } else {
                SLF4jLogger.getLogger(DigiSpine.class).warn("Topic {} does not exist", topic);
            }
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void deleteTopics(){
        try (AdminClient admin = AdminClient.create(Collections.singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker.getBootstrapServers()))) {
            //Do not delete system topics starting with the leading '_'
            var topics = admin.listTopics().names().get().stream().filter( element -> !element.startsWith("_")).toList();
            admin.deleteTopics(topics);
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}

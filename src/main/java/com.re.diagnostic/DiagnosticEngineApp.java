package com.re.diagnostic;

import com.re.diagnostic.db.DtcRepository;
import com.re.diagnostic.db.DtcStateCache;
import com.re.diagnostic.db.PostgresService;
import com.re.diagnostic.sync.SyncTopicListener;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.CompletableFuture;
import com.re.diagnostic.rules.RuleCache;
import com.re.diagnostic.rules.RuleLoader;
import com.re.diagnostic.rules.RuleEvaluator;
import org.apache.logging.log4j.core.config.Configurator;

public class DiagnosticEngineApp {

    private static final Logger logger = LogManager.getLogger(DiagnosticEngineApp.class);
    private static volatile boolean running = true;

    public static void main(String[] args) {

        logger.info("Starting Diagnostic Engine Application");
        String APPLICATION_ID = System.getenv().getOrDefault("APPLICATION_ID", "diagnostic-enginess112435s");
        String KAFKA_BOOTSTRAP_BROKER = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_BROKER", "localhost:9092");
        String INPUT_TOPIC = System.getenv().getOrDefault("INPUT_TOPIC", "get-telemetry-data");
        String OUTPUT_TOPIC = System.getenv().getOrDefault("OUTPUT_TOPIC", "telemetry-output-data");
        String NOTIFICATION_OUTPUT_TOPIC = System.getenv().getOrDefault("NOTIFICATION_OUTPUT_TOPIC", "dtc-alert-topic");
        String SYNC_TOPIC = System.getenv().getOrDefault("SYNC_TOPIC", "sync-topic");
        String SYNC_CONSUMER_GROUP = System.getenv().getOrDefault("SYNC_CONSUMER_GROUP",
                "sync-consumer-group") + "-" + System.currentTimeMillis();
        String POSTGRES_HOST = System.getenv().getOrDefault("POSTGRES_HOST", "3.108.167.239");
        String POSTGRES_PORT = System.getenv().getOrDefault("POSTGRES_PORT", "5432");
        String POSTGRES_DB_NAME = System.getenv().getOrDefault("POSTGRES_DB_NAME", "RE");
        String POSTGRES_DB_USERNAME = System.getenv().getOrDefault("POSTGRES_DB_USERNAME", "postgres");
        String POSTGRES_DB_PASSWORD = System.getenv().getOrDefault("POSTGRES_DB_PASSWORD", "tracking2018");
        String batch_size = System.getenv("BATCH_SIZE") != null ? System.getenv("BATCH_SIZE") : "500";
        String linger_ms = System.getenv("LINGER_MS") != null ? System.getenv("LINGER_MS") : "20000";
        String logLevel = System.getenv("LOG4J_LEVEL") != null ? System.getenv("LOG4J_LEVEL") : "INFO";
        Configurator.setRootLevel(Level.toLevel(logLevel));

        logger.info("Application ID       : {}", APPLICATION_ID);
        logger.info("Kafka Broker         : {}", KAFKA_BOOTSTRAP_BROKER);
        logger.info("Input Topic          : {}", INPUT_TOPIC);
        logger.info("Output Topic         : {}", OUTPUT_TOPIC);
        logger.info("Notification Topic   : {}", NOTIFICATION_OUTPUT_TOPIC);
        logger.info("Sync Topic           : {}", SYNC_TOPIC);
        logger.info("Postgres DB          : {}:{} / {}", POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB_NAME);

        PostgresService postgresService = new PostgresService(POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB_NAME,
                POSTGRES_DB_USERNAME, POSTGRES_DB_PASSWORD);
        logger.info("Postgres connection established successfully");

        DtcRepository dtcRepository = new DtcRepository(postgresService);
        DtcStateCache dtcStateCache = new DtcStateCache(dtcRepository);
        logger.info("DTC state cache initialized with Caffeine LoadingCache");

        RuleCache ruleCache = new RuleCache();
        RuleLoader ruleLoader = new RuleLoader(postgresService, ruleCache);
        logger.info("Loading DTC rules from database to cache...");
        ruleLoader.loadAllRules();
        logger.info("Rule loading completed. Total rules loaded to cache: {}", ruleCache.size());

        // Start sync topic listener
        Properties syncProps = new Properties();
        syncProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_BROKER);
        syncProps.put(ConsumerConfig.GROUP_ID_CONFIG, SYNC_CONSUMER_GROUP);
        syncProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        syncProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        syncProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        new Thread(new SyncTopicListener(syncProps, SYNC_TOPIC, ruleLoader, ruleCache)).start();

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_BROKER);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        if (null != batch_size) {
            producerProps.put("batch.size", batch_size);
        }
        if (null != linger_ms) {
            producerProps.put("linger.ms", linger_ms);
        }

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        RuleEvaluator ruleEvaluator = new RuleEvaluator(ruleCache);

        DiagnosticEngineService diagnosticEngine = new DiagnosticEngineService(ruleEvaluator, dtcRepository,
                dtcStateCache, producer, OUTPUT_TOPIC, NOTIFICATION_OUTPUT_TOPIC);
        logger.info("Diagnostic Engine initialized successfully");

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_BROKER);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, APPLICATION_ID);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);

        consumer.subscribe(Collections.singletonList(INPUT_TOPIC));
        logger.info("Subscribed to input topic: {}", INPUT_TOPIC);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down Diagnostic Engine...");
            running = false;
            consumer.wakeup();
        }));

        logger.info("Diagnostic Engine is now running...");
        ExecutorService threadPool = Executors.newFixedThreadPool(20);
        
        try {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                
                if (records.isEmpty()) {
                    continue;
                }

                List<CompletableFuture<Void>> tasks = new ArrayList<>();

                for (ConsumerRecord<String, String> record : records) {
                    CompletableFuture<Void> task = CompletableFuture.runAsync(() -> {
                        try {
                            logger.debug("Processing record offset={}, partition={}",
                                    record.offset(), record.partition());
                            diagnosticEngine.process(record.value());
                        } catch (Exception e) {
                            logger.error("Failed processing offset {}", record.offset(), e);
                        }
                    }, threadPool);
                    tasks.add(task);
                }
                
                // Wait for all tasks in the batch to finish
                CompletableFuture.allOf(tasks.toArray(new CompletableFuture[0])).join();
                
                // Safely commit offsets only after entire batch is successfully processed
                consumer.commitSync();
            }
        } catch (WakeupException e) {
            logger.info("Consumer wakeup triggered for shutdown");
        } catch (Exception e) {
            logger.error("Unexpected error in processing loop", e);
        } finally {
            threadPool.shutdown();
            logger.info("Closing Kafka consumer and producer");
            consumer.close();
            producer.close();
            postgresService.shutdown();
            logger.info("Resources closed. Application stopped.");
        }
    }

}
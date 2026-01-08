package com.fraudpipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fraudpipeline.events.FraudScore;
import com.fraudpipeline.events.Transaction;
import com.fraudpipeline.features.FeatureEnricher;
import com.fraudpipeline.rules.VelocityRule;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Real-Time Fraud Detection Pipeline
 * 
 * Main Flink job that:
 * 1. Consumes transactions from Kafka
 * 2. Computes real-time features (velocity, amounts, etc.)
 * 3. Applies fraud detection rules (CEP patterns)
 * 4. Scores transactions with ML model
 * 5. Outputs scored transactions to Kafka and Redis
 * 
 * Architecture:
 * [Kafka: transactions] -> [Feature Enrichment] -> [Rule Engine] -> [ML Scoring] -> [Kafka: fraud-scores]
 *                                                                                 -> [Redis: features]
 * 
 * @author Gunner | Portfolio Project Q2 2026
 */
public class FraudDetectionJob {
    private static final Logger LOG = LoggerFactory.getLogger(FraudDetectionJob.class);
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    // Configuration (would be externalized in production)
    private static final String KAFKA_BOOTSTRAP_SERVERS = 
            System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092");
    private static final String INPUT_TOPIC = 
            System.getenv().getOrDefault("INPUT_TOPIC", "transactions");
    private static final String OUTPUT_TOPIC = 
            System.getenv().getOrDefault("OUTPUT_TOPIC", "fraud-scores");
    private static final String REDIS_HOST = 
            System.getenv().getOrDefault("REDIS_HOST", "redis");
    private static final int REDIS_PORT = 
            Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Fraud Detection Pipeline");
        LOG.info("Kafka: {}, Input: {}, Output: {}", KAFKA_BOOTSTRAP_SERVERS, INPUT_TOPIC, OUTPUT_TOPIC);

        // Initialize Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Configure checkpointing for exactly-once semantics
        env.enableCheckpointing(60000); // Checkpoint every 60 seconds
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(120000);
        
        // Set parallelism (can be overridden by Flink cluster)
        env.setParallelism(2);

        // =========================================================================
        // SOURCE: Kafka Consumer
        // =========================================================================
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(INPUT_TOPIC)
                .setGroupId("fraud-detection-processor")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Watermark strategy for event time processing
        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withIdleness(Duration.ofMinutes(1));

        // Create source stream
        DataStream<String> rawStream = env.fromSource(
                kafkaSource, 
                watermarkStrategy, 
                "Kafka Transaction Source"
        );

        // =========================================================================
        // TRANSFORMATION: Parse JSON to Transaction
        // =========================================================================
        DataStream<Transaction> transactionStream = rawStream
                .map(json -> {
                    try {
                        return MAPPER.readValue(json, Transaction.class);
                    } catch (Exception e) {
                        LOG.warn("Failed to parse transaction: {}", e.getMessage());
                        return null;
                    }
                })
                .filter(txn -> txn != null)
                .name("Parse Transactions");

        // Log throughput (for demo purposes)
        transactionStream
                .map(txn -> {
                    LOG.debug("Processing: {} from {} - {} {}", 
                            txn.getTransactionId(), 
                            txn.getUserId(),
                            txn.getAmount(),
                            txn.getCurrency());
                    return txn;
                })
                .name("Log Throughput");

        // =========================================================================
        // FEATURE ENRICHMENT: Compute real-time features
        // =========================================================================
        DataStream<Transaction> enrichedStream = transactionStream
                .keyBy(Transaction::getUserId)
                .process(new FeatureEnricher(REDIS_HOST, REDIS_PORT))
                .name("Feature Enrichment");

        // =========================================================================
        // RULE ENGINE: Apply velocity rules (windowed aggregation)
        // =========================================================================
        DataStream<FraudScore> velocityAlerts = enrichedStream
                .keyBy(Transaction::getUserId)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .process(new VelocityRule(10)) // Alert if > 10 txns in 1 hour
                .name("Velocity Rule");

        // =========================================================================
        // SCORING: Combine rules into final fraud score
        // =========================================================================
        DataStream<FraudScore> scoredStream = enrichedStream
                .keyBy(Transaction::getUserId)
                .map(txn -> {
                    FraudScore score = FraudScore.fromTransaction(txn);
                    
                    // Simple rule-based scoring (ML model integration in Phase 2)
                    double ruleScore = 0.0;
                    
                    // New device + high value
                    if (txn.isFirstDeviceTxn() && txn.getAmountInUsd().doubleValue() > 200) {
                        score.addRuleTriggered("NEW_DEVICE_HIGH_VALUE");
                        ruleScore += 0.3;
                    }
                    
                    // New account + high value
                    if (txn.getAccountAgeDays() < 7 && txn.getAmountInUsd().doubleValue() > 100) {
                        score.addRuleTriggered("NEW_ACCOUNT_HIGH_VALUE");
                        ruleScore += 0.2;
                    }
                    
                    // Cross-border transaction
                    if (txn.getRecipientCountry() != null && 
                            !txn.getRecipientCountry().equals(txn.getCountryCode())) {
                        score.addRuleTriggered("CROSS_BORDER");
                        ruleScore += 0.1;
                    }
                    
                    score.setFraudScore(Math.min(1.0, ruleScore));
                    score.calculateRiskLevel();
                    
                    return score;
                })
                .name("Score Transactions");

        // =========================================================================
        // SINK: Output to Kafka
        // =========================================================================
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(OUTPUT_TOPIC)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        scoredStream
                .map(score -> MAPPER.writeValueAsString(score))
                .sinkTo(kafkaSink)
                .name("Kafka Fraud Score Sink");

        // Also output velocity alerts
        velocityAlerts
                .map(score -> MAPPER.writeValueAsString(score))
                .sinkTo(kafkaSink)
                .name("Kafka Velocity Alert Sink");

        // Log high-risk transactions
        scoredStream
                .filter(score -> score.getRiskLevel() == FraudScore.RiskLevel.HIGH || 
                               score.getRiskLevel() == FraudScore.RiskLevel.CRITICAL)
                .map(score -> {
                    LOG.warn("HIGH RISK: {} - Score: {}, Rules: {}", 
                            score.getTransactionId(), 
                            score.getFraudScore(),
                            score.getRulesTriggered());
                    return score;
                })
                .name("Log High Risk");

        // =========================================================================
        // EXECUTE
        // =========================================================================
        env.execute("Fraud Detection Pipeline");
    }
}

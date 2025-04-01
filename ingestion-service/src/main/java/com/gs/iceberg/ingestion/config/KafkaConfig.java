package com.gs.iceberg.ingestion.config;

import lombok.Data;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration class for Kafka consumer settings.
 * Supports Kerberos authentication for secure Kafka clusters.
 */
@Configuration
@ConfigurationProperties(prefix = "kafka")
@Data
public class KafkaConfig {

    private String bootstrapServers;
    private String topic;
    private String consumerGroup;
    private String securityProtocol = SecurityProtocol.PLAINTEXT.name();
    private String saslMechanism = "GSSAPI";
    private String keytabPath;
    private String principal;
    private int consumptionRate = 100; // Default: 100 messages/minute

    /**
     * Creates a Kafka consumer factory with the configured properties.
     * If Kerberos authentication is enabled, it configures the necessary security settings.
     */
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        int pollIntervalMs = 60000 / consumptionRate;
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Math.max(1, consumptionRate / 60));
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, pollIntervalMs);
        
        if (SecurityProtocol.SASL_PLAINTEXT.name().equals(securityProtocol) || 
            SecurityProtocol.SASL_SSL.name().equals(securityProtocol)) {
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
            props.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
            props.put(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, "kafka");
            
            if (keytabPath != null && !keytabPath.isEmpty() && 
                principal != null && !principal.isEmpty()) {
                System.setProperty("java.security.auth.login.config", 
                                  createJaasConfigFile());
                System.setProperty("java.security.krb5.conf", 
                                  "/etc/krb5.conf");
            }
        }
        
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * Creates a Kafka listener container factory with the configured consumer factory.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }
    
    /**
     * Creates a JAAS configuration file for Kerberos authentication.
     * This is required for Kafka to authenticate with Kerberos.
     */
    private String createJaasConfigFile() {
        try {
            String jaasPath = "/tmp/kafka_jaas.conf";
            java.nio.file.Path path = java.nio.file.Paths.get(jaasPath);
            
            StringBuilder jaasConfig = new StringBuilder();
            jaasConfig.append("KafkaClient {\n");
            jaasConfig.append("  com.sun.security.auth.module.Krb5LoginModule required\n");
            jaasConfig.append("  useKeyTab=true\n");
            jaasConfig.append("  storeKey=true\n");
            jaasConfig.append("  keyTab=\"").append(keytabPath).append("\"\n");
            jaasConfig.append("  principal=\"").append(principal).append("\";\n");
            jaasConfig.append("};");
            
            java.nio.file.Files.write(path, jaasConfig.toString().getBytes());
            return jaasPath;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create JAAS config file", e);
        }
    }
    
    /**
     * Creates a Kafka producer factory with the configured properties.
     * Used for sending messages to the error queue.
     */
    @Bean
    public org.springframework.kafka.core.ProducerFactory<String, String> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
                 org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
                 org.apache.kafka.common.serialization.StringSerializer.class);
        
        if (SecurityProtocol.SASL_PLAINTEXT.name().equals(securityProtocol) || 
            SecurityProtocol.SASL_SSL.name().equals(securityProtocol)) {
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
            props.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
            props.put(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, "kafka");
        }
        
        return new org.springframework.kafka.core.DefaultKafkaProducerFactory<>(props);
    }
    
    /**
     * Creates a Kafka template for sending messages to Kafka.
     */
    @Bean
    public org.springframework.kafka.core.KafkaTemplate<String, String> kafkaTemplate() {
        return new org.springframework.kafka.core.KafkaTemplate<>(producerFactory());
    }
}

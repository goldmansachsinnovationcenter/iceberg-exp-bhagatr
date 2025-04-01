package com.gs.iceberg.ingestion.config;

import lombok.Data;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration class for Apache Spark.
 * Sets up the Spark session with the necessary configurations for Iceberg integration.
 */
@Configuration
@ConfigurationProperties(prefix = "spark")
@Data
public class SparkConfig {

    private String appName = "Kafka-Iceberg-Ingestion";
    private String master = "local[*]";
    private String warehouseLocation;
    private String hadoopConfDir;
    private String keytabPath;
    private String principal;

    /**
     * Creates a Spark session with the configured properties.
     * Configures Iceberg integration and Kerberos authentication if enabled.
     */
    @Bean
    public SparkSession sparkSession() {
        SparkConf conf = new SparkConf()
                .setAppName(appName)
                .setMaster(master)
                .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                .set("spark.sql.catalog.spark_catalog.type", "hive")
                .set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
                .set("spark.sql.catalog.local.type", "hadoop")
                .set("spark.sql.catalog.local.warehouse", warehouseLocation);

        if (keytabPath != null && !keytabPath.isEmpty() && 
            principal != null && !principal.isEmpty()) {
            System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");
            System.setProperty("hadoop.conf.dir", hadoopConfDir);
            
            conf.set("spark.hadoop.hadoop.security.authentication", "kerberos");
            conf.set("spark.hadoop.hadoop.security.authorization", "true");
            conf.set("spark.hadoop.yarn.resourcemanager.principal", principal);
            conf.set("spark.kerberos.keytab", keytabPath);
            conf.set("spark.kerberos.principal", principal);
        }

        return SparkSession.builder()
                .config(conf)
                .getOrCreate();
    }
}

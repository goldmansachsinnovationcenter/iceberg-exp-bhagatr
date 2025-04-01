package com.gs.iceberg.ingestion.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration class for storage settings.
 * Supports both HDFS and S3 as backend storage with Kerberos authentication.
 */
@Configuration
@ConfigurationProperties(prefix = "storage")
@Data
public class StorageConfig {

    private String type = "HDFS"; // Default: HDFS, can be "HDFS" or "S3"
    private String basePath;
    private int retentionDays = 10; // Default: 10 days
    
    private String hdfsKeytabPath;
    private String hdfsPrincipal;
    private String hdfsConfDir;
    
    private String s3AccessKey;
    private String s3SecretKey;
    private String s3Region;
    private String s3Endpoint;
    
    /**
     * Returns the storage path based on the configured type.
     */
    public String getStoragePath() {
        if ("S3".equalsIgnoreCase(type)) {
            return "s3a://" + basePath;
        } else {
            return "hdfs://" + basePath;
        }
    }
    
    /**
     * Configures the system properties for Kerberos authentication with HDFS.
     */
    public void configureHdfsKerberos() {
        if (hdfsKeytabPath != null && !hdfsKeytabPath.isEmpty() && 
            hdfsPrincipal != null && !hdfsPrincipal.isEmpty()) {
            System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");
            System.setProperty("hadoop.conf.dir", hdfsConfDir);
            System.setProperty("hadoop.security.authentication", "kerberos");
            System.setProperty("hadoop.security.authorization", "true");
            System.setProperty("dfs.namenode.kerberos.principal", hdfsPrincipal);
        }
    }
    
    /**
     * Configures the system properties for S3 authentication.
     */
    public void configureS3Authentication() {
        if (s3AccessKey != null && !s3AccessKey.isEmpty() && 
            s3SecretKey != null && !s3SecretKey.isEmpty()) {
            System.setProperty("fs.s3a.access.key", s3AccessKey);
            System.setProperty("fs.s3a.secret.key", s3SecretKey);
            System.setProperty("fs.s3a.endpoint", s3Endpoint);
            System.setProperty("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
            System.setProperty("fs.s3a.aws.credentials.provider", 
                              "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
            
            if (s3Region != null && !s3Region.isEmpty()) {
                System.setProperty("fs.s3a.region", s3Region);
            }
        }
    }
}

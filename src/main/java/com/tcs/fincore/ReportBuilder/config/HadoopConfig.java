package com.tcs.fincore.ReportBuilder.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

@Configuration
@Slf4j
public class HadoopConfig {
    @Value("${hadoop.fs.uri}")
    private String hadoopFsUri;
    @Value("${hadoop.fs.user}")
    private String hadoopUser;

    @Bean
    public FileSystem fileSystem() {

        log.info("Initializing HDFS connection to: {} as user: {}", hadoopFsUri, hadoopUser);
        org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();

        // 1. Set the default FileSystem URI
        config.set("fs.defaultFS", hadoopFsUri);

        // 2. OPTIMIZATION: Disable client-side permission checking.
        // This relies on the server to check permissions, preventing local OS user mismatch errors.
//        config.set("dfs.client.use.datanode.hostname", "true");

        // 3. ROBUSTNESS: Set timeouts (default is often too high or infinite)
        config.set("ipc.client.connect.timeout", "5000"); // 5 seconds
        config.set("ipc.client.rpc-timeout.ms", "30000"); // 30 seconds

        // 4. Replication adjustment (optional, prevents warnings in single-node dev setups)
        // config.set("dfs.replication", "1");
        try {
            // Returns a FileSystem instance. The FileSystem class handles caching internally.
            // Spring will manage the lifecycle (closing it on shutdown).
            return FileSystem.get(new URI(hadoopFsUri), config, hadoopUser);

        } catch (IOException | InterruptedException | URISyntaxException e) {

            log.error("CRITICAL: Failed to connect to HDFS at {}", hadoopFsUri, e);
            throw new RuntimeException("Could not initialize Hadoop FileSystem", e);
        }
    }
}
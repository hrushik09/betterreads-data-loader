package io.hrushik09.betterreadsdataloader;

import com.datastax.oss.driver.api.core.CqlSession;
import org.springframework.beans.factory.annotation.Value;

import java.nio.file.Paths;

public class Main {
    @Value("${spring.data.cassandra.username}")
    private static String username;

    @Value("${spring.data.cassandra.password}")
    private static String password;

    @Value("${spring.data.cassandra.keyspace-name}")
    private static String keyspaceName;

    @Value("${datastax.astra.secure-connect-bundle}")
    private static String secureConnectBundle;

    public static void main(String[] args) {
        try (CqlSession session = CqlSession.builder()
                .withCloudSecureConnectBundle(Paths.get(secureConnectBundle))
                .withAuthCredentials(username, password)
                .withKeyspace(keyspaceName)
                .build()) {
        }
    }
}

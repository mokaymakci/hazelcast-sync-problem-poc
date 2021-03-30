package com.poc.hazelcast;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientConnectionStrategyConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.config.ConnectionRetryConfig;
import com.hazelcast.config.*;
import com.hazelcast.core.HazelcastInstance;

import java.util.Arrays;

public class HazelcastApp {

    public static void main(String[] args) {
        String members = args[0];
        HazelcastInstance hazelcastInstance = null;
        try {
            hazelcastInstance = HazelcastClient.newHazelcastClient(createClientConfig(members));

            // Gets the wrong number of objects
            var distributedObjects = hazelcastInstance.getDistributedObjects();

            for (var o : distributedObjects) {
                System.out.println(o.getName());
            }
            System.out.println("Total number of objects:" + distributedObjects.stream().count());
            //hazelcastInstance.shutdown();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static ClientConfig createClientConfig(String members) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setInstanceName("Hazelcast.PoC");
        clientConfig.setProperty("hazelcast.client.statistics.enabled", "true");
        setNetworkConfig(clientConfig, members);
        setConnectionRetryConfig(clientConfig);
        setNearCacheConfig(clientConfig);
        return clientConfig;
    }

    private static void setConnectionRetryConfig(ClientConfig clientConfig) {
        ClientConnectionStrategyConfig connectionStrategyConfig = clientConfig.getConnectionStrategyConfig();
        ConnectionRetryConfig connectionRetryConfig = connectionStrategyConfig.getConnectionRetryConfig();
        connectionRetryConfig.setInitialBackoffMillis(1000)
                .setMaxBackoffMillis(60000)
                .setMultiplier(2)
                .setFailOnMaxBackoff(false)
                .setJitter(0.2)
                .setEnabled(true);
    }

    private static void setNetworkConfig(ClientConfig clientConfig, String members) {

        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        String[] membersArray = Arrays.stream(members.split(",")).map(String::trim).toArray(String[]::new);

        networkConfig.addAddress(membersArray)
                .addOutboundPortDefinition("34700-34705")
                .setRedoOperation(true)
                .setConnectionTimeout(5000);
        GroupConfig groupConfig = clientConfig.getGroupConfig();
        String groupConfigName = "dev";
        groupConfig.setName(groupConfigName);
    }

    // Near cache doesn't sync with the Hz servers
    private static void setNearCacheConfig(ClientConfig clientConfig) {
        EvictionConfig evictionConfigLFU = new EvictionConfig()
                .setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.ENTRY_COUNT)
                .setEvictionPolicy(EvictionPolicy.LFU)
                .setSize(10000);

        EvictionConfig evictionConfigLRU = new EvictionConfig()
                .setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.ENTRY_COUNT)
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setSize(10000);

        var nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setName("CustomerIssuedJwt")
                .setInMemoryFormat(InMemoryFormat.BINARY)
                .setSerializeKeys(false)
                .setInvalidateOnChange(true)
                .setTimeToLiveSeconds(1000) // 16.6 minutes
                .setMaxIdleSeconds(200); // 3.33 minutes
        nearCacheConfig.setEvictionConfig(evictionConfigLRU);
        clientConfig.addNearCacheConfig(nearCacheConfig);

        for (var cache : new String[]{"CustomerSessionKey", "DeviceIssuedJwt", "ValidCustomerDevice", "PhoneNumber", "CustomerDevice"}) {
            nearCacheConfig = new NearCacheConfig();
            nearCacheConfig.setName(cache)
                    .setInMemoryFormat(InMemoryFormat.BINARY)
                    .setSerializeKeys(false)
                    .setInvalidateOnChange(true)
                    .setTimeToLiveSeconds(0)
                    .setMaxIdleSeconds(300); // 2 hours 7200
            nearCacheConfig.setEvictionConfig(evictionConfigLFU);
            clientConfig.addNearCacheConfig(nearCacheConfig);
        }
    }
}

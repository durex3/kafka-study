package com.durex.kafkastudy.admin;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;

import java.util.*;

/**
 * @author gelong
 * @date 2020/4/12 1:33
 */
public class AdminSample {

    public final static String TOPIC_NAME = "durex";

    /**
     * 设置 AdminClient
     * @return AdminClient
     */
    public static AdminClient adminClient() {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.104:9092");
        return AdminClient.create(properties);
    }

    /**
     * 创建Topic实例
     * @throws Exception 异常
     */
    public static void createTopics() throws Exception {
        AdminClient adminClient = adminClient();
        NewTopic newTopic = new NewTopic(TOPIC_NAME, 1, (short)1);
        CreateTopicsResult topics = adminClient.createTopics(Collections.singletonList(newTopic));
        topics.all().get();
        System.out.println("CreateTopicsResult: " + topics.values());
    }

    /**
     * 获取Topic列表
     * @throws Exception 异常
     */
    public static void topicsLists() throws Exception {
        AdminClient adminClient = adminClient();
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(true);
        // ListTopicsResult listTopicsResult = adminClient.listTopics();
        ListTopicsResult listTopicsResult = adminClient.listTopics(options);
        Set<String> names = listTopicsResult.names().get();
        names.forEach(System.out::println);
        Collection<TopicListing> topicListings = listTopicsResult.listings().get();
        topicListings.forEach(System.out::println);
    }

    /**
     * 删除Topic
     * @throws Exception 异常
     */
    public static void deleteTopics() throws Exception {
        AdminClient adminClient = adminClient();
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Collections.singletonList(TOPIC_NAME));
        deleteTopicsResult.all().get();
    }

    /**
     * 描述Topic
     * @throws Exception 异常
     */
    public static void describeTopics() throws Exception {
        AdminClient adminClient = adminClient();
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singleton(TOPIC_NAME));
        Map<String, TopicDescription> stringTopicDescriptionMap = describeTopicsResult.all().get();
        Set<Map.Entry<String, TopicDescription>> entries = stringTopicDescriptionMap.entrySet();
        entries.forEach(entry -> System.out.println(entry.getKey() + ", " + entry.getValue()));
    }

    /**
     * 查询配置信息
     * @throws Exception 异常
     */
    public static void describeConfig() throws Exception {
        AdminClient adminClient = adminClient();
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);
        DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(Collections.singletonList(configResource));
        Map<ConfigResource, Config> configResourceConfigMap = describeConfigsResult.all().get();
        Set<Map.Entry<ConfigResource, Config>> entries = configResourceConfigMap.entrySet();
        entries.forEach(entry -> System.out.println("ConfigResource: " + entry.getKey() + ", " + entry.getValue()));
    }

    /**
     * 修改config信息
     * @throws Exception 异常
     */
    public static void alterConfig() throws Exception {
        AdminClient adminClient = adminClient();
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);

       /* Map<ConfigResource, Config> configResourceConfigMap = new HashMap<>(16);
        Config config = new Config(Collections.singleton(new ConfigEntry("preallocate", "true")));
        configResourceConfigMap.put(configResource, config);
        AlterConfigsResult alterConfigsResult = adminClient.alterConfigs(configResourceConfigMap);
        alterConfigsResult.all().get();*/

        Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>(16);
        AlterConfigOp alterConfigOp = new AlterConfigOp(
                new ConfigEntry("preallocate", "false"), AlterConfigOp.OpType.SET);
        configs.put(configResource, Collections.singletonList(alterConfigOp));
        AlterConfigsResult alterConfigsResult = adminClient.incrementalAlterConfigs(configs);
        alterConfigsResult.all().get();
    }

    /**
     * 增加 partition数量
     * @param partitions 数量
     * @throws Exception 异常
     */
    public static void incrPartitions(int partitions) throws Exception {
        AdminClient adminClient = adminClient();
        Map<String, NewPartitions> partitionsMap = new HashMap<>(16);
        NewPartitions newPartitions = NewPartitions.increaseTo(partitions);
        partitionsMap.put(TOPIC_NAME, newPartitions);
        CreatePartitionsResult result = adminClient.createPartitions(partitionsMap);
        result.all().get();
    }

    public static void main(String[] args) throws Exception {
        describeTopics();
    }
}

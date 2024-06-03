import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class KafkaTopicFetcher {
    public static String[] getTopics() {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        try (AdminClient adminClient = AdminClient.create(config)) {
            Set<String> topics = adminClient.listTopics().names().get();
            return topics.toArray(new String[0]);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            return new String[]{};
        }
    }
}

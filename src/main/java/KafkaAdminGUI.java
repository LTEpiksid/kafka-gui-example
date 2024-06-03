import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class KafkaAdminGUI extends JFrame {
    private JTextField topicNameField;
    private JTextArea outputArea;
    private JButton createTopicButton;
    private JButton listTopicsButton;
    private JButton deleteTopicButton;

    private Admin kafkaAdmin;

    public KafkaAdminGUI() {
        setTitle("Kafka Admin");
        setSize(600, 400);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLayout(new GridLayout(8, 1));

        topicNameField = new JTextField();
        outputArea = new JTextArea();
        outputArea.setEditable(false);

        createTopicButton = new JButton("Create Topic");
        listTopicsButton = new JButton("List Topics");
        deleteTopicButton = new JButton("Delete Topic");

        add(new JLabel("Topic Name:"));
        add(topicNameField);
        add(createTopicButton);
        add(listTopicsButton);
        add(deleteTopicButton);
        add(new JScrollPane(outputArea));

        createTopicButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                createTopic();
            }
        });

        listTopicsButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                listTopics();
            }
        });

        deleteTopicButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                deleteTopic();
            }
        });

        setVisible(true);
        initializeAdminClient();
    }

    private void initializeAdminClient() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaAdmin = Admin.create(props);
    }

    private void createTopic() {
        String topicName = topicNameField.getText();
        int partitions = 1; // default number of partitions
        short replicationFactor = 1; // default replication factor

        NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
        try {
            kafkaAdmin.createTopics(Collections.singletonList(newTopic)).all().get();
            outputArea.append("Topic created: " + topicName + "\n");
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            outputArea.append("Failed to create topic: " + e.getMessage() + "\n");
        }
    }

    private void listTopics() {
        try {
            Set<String> topics = kafkaAdmin.listTopics().names().get();
            outputArea.append("Topics: " + topics + "\n");
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            outputArea.append("Failed to list topics: " + e.getMessage() + "\n");
        }
    }

    private void deleteTopic() {
        String topicName = topicNameField.getText();
        try {
            kafkaAdmin.deleteTopics(Collections.singletonList(topicName)).all().get();
            outputArea.append("Topic deleted: " + topicName + "\n");
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            outputArea.append("Failed to delete topic: " + e.getMessage() + "\n");
        }
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(KafkaAdminGUI::new);
    }
}


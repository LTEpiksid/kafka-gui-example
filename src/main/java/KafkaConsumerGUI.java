import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerGUI extends JFrame {

    private JTextArea textArea;
    private JComboBox<String> topicComboBox;
    private JButton refreshTopicsButton;
    private JButton startConsumingButton;
    private volatile boolean consuming = false;

    public KafkaConsumerGUI() {
        setTitle("Kafka Consumer");
        setSize(500, 400);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        textArea = new JTextArea();
        textArea.setEditable(false);
        topicComboBox = new JComboBox<>();
        refreshTopicsButton = new JButton("Refresh Topics");
        startConsumingButton = new JButton("Start Consuming");

        JPanel panel = new JPanel(new BorderLayout());
        JPanel topPanel = new JPanel(new BorderLayout());
        topPanel.add(topicComboBox, BorderLayout.CENTER);
        topPanel.add(refreshTopicsButton, BorderLayout.EAST);
        panel.add(topPanel, BorderLayout.NORTH);
        panel.add(startConsumingButton, BorderLayout.SOUTH);

        add(panel, BorderLayout.NORTH);
        add(new JScrollPane(textArea), BorderLayout.CENTER);

        refreshTopicsButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                populateTopics();
            }
        });

        startConsumingButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                if (consuming) {
                    consuming = false;
                    startConsumingButton.setText("Start Consuming");
                } else {
                    consuming = true;
                    startConsumingButton.setText("Stop Consuming");
                    new Thread(KafkaConsumerGUI.this::consumeMessages).start();
                }
            }
        });

        setVisible(true);
        populateTopics();
    }

    private void populateTopics() {
        topicComboBox.removeAllItems();
        String[] topics = KafkaTopicFetcher.getTopics();
        for (String topic : topics) {
            topicComboBox.addItem(topic);
        }
    }

    private void consumeMessages() {
        String topic = (String) topicComboBox.getSelectedItem();
        if (topic == null) {
            textArea.append("No topic selected.\n");
            return;
        }

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));

            while (consuming) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    textArea.append("Received message from " + topic + ": " + record.value() + " with offset: " + record.offset() + "\n");
                    textArea.setCaretPosition(textArea.getDocument().getLength());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(KafkaConsumerGUI::new);
    }
}

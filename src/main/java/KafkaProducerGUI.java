import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerGUI extends JFrame {

    private JTextArea textArea;
    private JTextField textField;
    private JButton sendButton;
    private KafkaProducer<String, String> producer;

    public KafkaProducerGUI() {
        setTitle("Kafka Producer");
        setSize(500, 400);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        textArea = new JTextArea();
        textArea.setEditable(false);
        textField = new JTextField();
        sendButton = new JButton("Send");

        JPanel panel = new JPanel(new BorderLayout());
        panel.add(textField, BorderLayout.CENTER);
        panel.add(sendButton, BorderLayout.EAST);

        add(new JScrollPane(textArea), BorderLayout.CENTER);
        add(panel, BorderLayout.SOUTH);

        sendButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                sendMessage(textField.getText());
                textField.setText("");
            }
        });

        setVisible(true);
        initializeProducer();
        produceInitialMessages();
    }

    private void initializeProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }

    private void produceInitialMessages() {
        for (int i = 0; i < 10; i++) {
            sendMessage("Hello, Kafka " + i);
        }
    }

    private void sendMessage(String value) {
        String key = "key-" + System.currentTimeMillis();
        ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", key, value);

        try {
            RecordMetadata metadata = producer.send(record).get();
            textArea.append("Sent message: " + value + " with offset: " + metadata.offset() + "\n");
            textArea.setCaretPosition(textArea.getDocument().getLength());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(KafkaProducerGUI::new);
    }
}

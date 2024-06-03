import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

public class KafkaServerManager extends JFrame {
    private static final String KAFKA_HOME = "C:\\kafka\\kafka_2.13-3.7.0";
    private static final String KAFKA_LOG_DIR = "C:\\tmp\\kafka-logs";
    private static final String ZOOKEEPER_DIR = "C:\\tmp\\zookeeper";

    private JLabel zookeeperStatusLabel;
    private JLabel kafkaStatusLabel;
    private JButton startZookeeperButton;
    private JButton startKafkaButton;
    private JButton quitButton;

    private Process zookeeperProcess;
    private Process kafkaProcess;

    public KafkaServerManager() {
        setTitle("Kafka Server Manager");
        setSize(400, 200);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLayout(new GridLayout(4, 1));

        zookeeperStatusLabel = new JLabel("ZooKeeper: Stopped");
        kafkaStatusLabel = new JLabel("Kafka: Stopped");
        startZookeeperButton = new JButton("Start ZooKeeper");
        startKafkaButton = new JButton("Start Kafka");
        quitButton = new JButton("Quit");

        add(zookeeperStatusLabel);
        add(startZookeeperButton);
        add(kafkaStatusLabel);
        add(startKafkaButton);
        add(quitButton);

        startZookeeperButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                startZooKeeper();
            }
        });

        startKafkaButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                startKafka();
            }
        });

        quitButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                stopServers();
                cleanUpDirectories();
                System.exit(0);
            }
        });

        setVisible(true);

        // Clean up existing directories when the application starts
        cleanUpDirectories();
    }

    private void startZooKeeper() {
        try {
            ProcessBuilder builder = new ProcessBuilder(KAFKA_HOME + "\\bin\\windows\\zookeeper-server-start.bat", KAFKA_HOME + "\\config\\zookeeper.properties");
            builder.directory(new File(KAFKA_HOME));
            builder.redirectErrorStream(true);
            zookeeperProcess = builder.start();

            new Thread(() -> {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(zookeeperProcess.getInputStream()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        System.out.println(line);
                        if (line.contains("binding to port")) {
                            SwingUtilities.invokeLater(() -> zookeeperStatusLabel.setText("ZooKeeper: Running"));
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void startKafka() {
        try {
            ProcessBuilder builder = new ProcessBuilder(KAFKA_HOME + "\\bin\\windows\\kafka-server-start.bat", KAFKA_HOME + "\\config\\server.properties");
            builder.directory(new File(KAFKA_HOME));
            builder.redirectErrorStream(true);
            kafkaProcess = builder.start();

            new Thread(() -> {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(kafkaProcess.getInputStream()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        System.out.println(line);
                        if (line.contains("KafkaServer")) {
                            SwingUtilities.invokeLater(() -> kafkaStatusLabel.setText("Kafka: Running"));
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void stopZooKeeper() {
        if (zookeeperProcess != null) {
            zookeeperProcess.destroy();
            try {
                zookeeperProcess.waitFor();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            forceKillProcess("zookeeper-server-start.bat");
            zookeeperStatusLabel.setText("ZooKeeper: Stopped");
        }
    }

    private void stopKafka() {
        if (kafkaProcess != null) {
            kafkaProcess.destroy();
            try {
                kafkaProcess.waitFor();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            forceKillProcess("kafka-server-start.bat");
            kafkaStatusLabel.setText("Kafka: Stopped");
        }
    }

    private void forceKillProcess(String processName) {
        try {
            ProcessBuilder builder = new ProcessBuilder("cmd.exe", "/c", "taskkill /IM java.exe /F");
            builder.redirectErrorStream(true);
            Process process = builder.start();

            new Thread(() -> {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        System.out.println(line);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();

            process.waitFor();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void stopServers() {
        stopKafka();
        stopZooKeeper();
    }

    private void cleanUpDirectories() {
        cleanUpDirectory(KAFKA_LOG_DIR, "Kafka logs");
        cleanUpDirectory(ZOOKEEPER_DIR, "ZooKeeper state");
    }

    private void cleanUpDirectory(String dirPath, String description) {
        Path path = Paths.get(dirPath);
        if (Files.exists(path)) {
            deleteDirectoryWithRetries(path, description);
        }
    }

    private void deleteDirectoryWithRetries(Path path, String description) {
        for (int i = 0; i < 10; i++) { // Retry up to 10 times
            try {
                deleteDirectoryRecursively(path);
                System.out.println(description + " directory deleted successfully.");
                break; // Exit loop if successful
            } catch (IOException e) {
                System.err.println("Failed to delete " + description + " directory (attempt " + (i + 1) + "): " + e.getMessage());
                try {
                    forceKillProcess("java.exe"); // Additional force kill attempt
                    TimeUnit.SECONDS.sleep(3); // Wait before retrying
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

    private void deleteDirectoryRecursively(Path path) throws IOException {
        if (Files.isDirectory(path)) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
                for (Path entry : stream) {
                    deleteDirectoryRecursively(entry);
                }
            }
        }
        Files.delete(path);
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            new KafkaServerManager();
            new KafkaAdminGUI();
        });
    }
}

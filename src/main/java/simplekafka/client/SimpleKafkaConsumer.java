package simplekafka.client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SimpleKafkaConsumer {
    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaConsumer.class.getName());
    private static final int MAX_BYTES = 1024 * 1024;
    private static final long POLL_INTERVAL_MS = 1000;
    
    private final SimpleKafkaClient client;
    private final String topic;
    private final int partition;
    private long currentOffset;
    private final AtomicBoolean running;
    private Thread consumerThread;
    
    public SimpleKafkaConsumer(String bootstrapBroker, int bootstrapPort, String topic, int partition) {
        this.client = new SimpleKafkaClient(bootstrapBroker, bootstrapPort);
        this.topic = topic;
        this.partition = partition;
        this.currentOffset = 0;
        this.running = new AtomicBoolean(false);
    }
    
    public void initialize() throws IOException {
        client.initialize();
    }
    
    public List<byte[]> poll() throws IOException {
        List<byte[]> messages = client.fetch(topic, partition, currentOffset, MAX_BYTES);
        if (!messages.isEmpty()) {
            currentOffset += messages.size();
        }
        return messages;
    }
    
    public void startConsuming(MessageHandler handler) {
        if (running.compareAndSet(false, true)) {
            consumerThread = new Thread(() -> {
                try {
                    while (running.get()) {
                        List<byte[]> messages = poll();
                        for (byte[] message : messages) {
                            String msg = new String(message, StandardCharsets.UTF_8);
                            handler.handle(msg);
                        }
                        if (messages.isEmpty()) {
                            Thread.sleep(POLL_INTERVAL_MS);
                        }
                    }
                } catch (Exception e) {
                    if (running.get()) {
                        LOGGER.log(Level.SEVERE, "Error in consumer loop", e);
                    }
                    running.set(false);
                }
            });
            consumerThread.setDaemon(true);
            consumerThread.start();
            LOGGER.info("Started consuming from topic: " + topic + ", partition: " + partition);
        }
    }
    
    public void stopConsuming() {
        if (running.compareAndSet(true, false)) {
            if (consumerThread != null) {
                try {
                    consumerThread.interrupt();
                    consumerThread.join(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            LOGGER.info("Stopped consuming from topic: " + topic + ", partition: " + partition);
        }
    }
    
    public void seek(long offset) {
        this.currentOffset = offset;
    }
    
    public long getCurrentOffset() {
        return currentOffset;
    }
    
    public interface MessageHandler {
        void handle(String message);
    }
    
    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Usage: SimpleKafkaConsumer <brokerHost> <brokerPort> <topic> <partition>");
            System.exit(1);
        }
        try {
            String host = args[0];
            int port = Integer.parseInt(args[1]);
            String topic = args[2];
            int partition = Integer.parseInt(args[3]);
            SimpleKafkaConsumer consumer = new SimpleKafkaConsumer(host, port, topic, partition);
            consumer.initialize();
            System.out.println("Consumer started for topic: " + topic + ", partition: " + partition);
            System.out.println("Waiting for messages...");
            consumer.startConsuming(message -> {
                System.out.println("Received: " + message);
            });
            Thread.sleep(Long.MAX_VALUE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

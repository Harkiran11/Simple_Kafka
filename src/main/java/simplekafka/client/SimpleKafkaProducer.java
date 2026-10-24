package simplekafka.client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class SimpleKafkaProducer {
    private final SimpleKafkaClient client;
    private final String topic;
    
    public SimpleKafkaProducer(String bootstrapBroker, int bootstrapPort, String topic) {
        this.client = new SimpleKafkaClient(bootstrapBroker, bootstrapPort);
        this.topic = topic;
    }
    
    public void initialize() throws IOException {
        client.initialize();
    }
    
    public long send(String message) throws IOException {
        byte[] data = message.getBytes(StandardCharsets.UTF_8);
        return client.send(topic, 0, data);
    }
    
    public long send(String message, int partition) throws IOException {
        byte[] data = message.getBytes(StandardCharsets.UTF_8);
        return client.send(topic, partition, data);
    }
    
    public void close() {
    }
    
    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: SimpleKafkaProducer <brokerHost> <brokerPort> <topic>");
            System.exit(1);
        }
        try {
            String host = args[0];
            int port = Integer.parseInt(args[1]);
            String topic = args[2];
            SimpleKafkaProducer producer = new SimpleKafkaProducer(host, port, topic);
            producer.initialize();
            System.out.println("Producer started for topic: " + topic);
            for (int i = 0; i < 10; i++) {
                String msg = "Message " + i + " at " + System.currentTimeMillis();
                long offset = producer.send(msg);
                System.out.println("Sent: " + msg + " (offset: " + offset + ")");
                Thread.sleep(500);
            }
            producer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

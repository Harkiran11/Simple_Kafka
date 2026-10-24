package simplekafka.client;

import simplekafka.broker.BrokerInfo;
import simplekafka.broker.Protocol;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleKafkaClient {
    private final String bootstrapBroker;
    private final int bootstrapPort;
    private final Map<String, TopicMetadata> topicMetadata;

    public SimpleKafkaClient(String bootstrapBroker, int bootstrapPort) {
        this.bootstrapBroker = bootstrapBroker;
        this.bootstrapPort = bootstrapPort;
        this.topicMetadata = new ConcurrentHashMap<>();
    }

    public void initialize() throws IOException {
        refreshMetadata();
    }

    public void refreshMetadata() throws IOException {
    }

    public long send(String topic, int partition, byte[] message) throws IOException {
        return sendToBroker(bootstrapBroker, bootstrapPort, topic, partition, message);
    }

    private long sendToBroker(String host, int port, String topic, int partition, byte[] message) throws IOException {
        try (SocketChannel channel = SocketChannel.open()) {
            channel.connect(new InetSocketAddress(host, port));
            ByteBuffer request = Protocol.encodeProduceRequest(topic, partition, message);
            channel.write(request);

            ByteBuffer response = ByteBuffer.allocate(10);
            channel.read(response);
            response.flip();

            byte responseType = response.get();
            if (responseType == Protocol.PRODUCE_RESPONSE) {
                long offset = response.getLong();
                byte status = response.get();
                if (status == 0) {
                    return offset;
                }
            }
            return -1;
        }
    }

    public List<byte[]> fetch(String topic, int partition, long offset, int maxBytes) throws IOException {
        return fetchFromBroker(bootstrapBroker, bootstrapPort, topic, partition, offset, maxBytes);
    }

    private List<byte[]> fetchFromBroker(String host, int port, String topic, int partition, long offset, int maxBytes) throws IOException {
        try (SocketChannel channel = SocketChannel.open()) {
            channel.connect(new InetSocketAddress(host, port));
            ByteBuffer request = Protocol.encodeFetchRequest(topic, partition, offset, maxBytes);
            channel.write(request);

            ByteBuffer response = ByteBuffer.allocate(1024 * 1024);
            channel.read(response);
            response.flip();

            byte responseType = response.get();
            if (responseType == Protocol.FETCH_RESPONSE) {
                int messageCount = response.getInt();
                List<byte[]> messages = new ArrayList<>();
                for (int i = 0; i < messageCount; i++) {
                    long msgOffset = response.getLong();
                    int messageSize = response.getInt();
                    byte[] message = new byte[messageSize];
                    response.get(message);
                    messages.add(message);
                }
                return messages;
            }
            return new ArrayList<>();
        }
    }

    public static class TopicMetadata {
        private final String name;
        private final List<PartitionInfo> partitions;

        public TopicMetadata(String name, List<PartitionInfo> partitions) {
            this.name = name;
            this.partitions = partitions;
        }
    }

    public static class PartitionInfo {
        private final int id;
        private final int leader;
        private final List<Integer> replicas;

        public PartitionInfo(int id, int leader, List<Integer> replicas) {
            this.id = id;
            this.leader = leader;
            this.replicas = replicas;
        }
    }
}

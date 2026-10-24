package simplekafka.broker;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SimpleKafkaBroker {
    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaBroker.class.getName());
    private static final String DATA_DIR = "data";
    private final int brokerId;
    private final String brokerHost;
    private final int brokerPort;
    private final Map<String, List<Partition>> topics;
    private final ExecutorService executor;
    private final ServerSocketChannel serverChannel;
    private final AtomicBoolean isRunning;
    private final AtomicBoolean isController;
    private final Map<Integer, BrokerInfo> clusterMetadata;
    private final ZookeeperClient zkClient;
    
    public SimpleKafkaBroker(int brokerId, String host, int port, int zkPort) throws IOException {
        this.brokerId = brokerId;
        this.brokerHost = host;
        this.brokerPort = port;
        this.topics = new ConcurrentHashMap<>();
        this.executor = Executors.newFixedThreadPool(10);
        this.serverChannel = ServerSocketChannel.open();
        this.isRunning = new AtomicBoolean(false);
        this.isController = new AtomicBoolean(false);
        this.clusterMetadata = new ConcurrentHashMap<>();
        File dataDir = new File(DATA_DIR + File.separator + brokerId);
        if (!dataDir.exists()) {
            dataDir.mkdirs();
        }
        this.zkClient = new ZookeeperClient("localhost", zkPort);
    }
    
    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: SimpleKafkaBroker <brokerId> <host> <port> [zkPort]");
            System.exit(1);
        }
        try {
            int brokerId = Integer.parseInt(args[0]);
            String host = args[1];
            int port = Integer.parseInt(args[2]);
            int zkPort = args.length > 3 ? Integer.parseInt(args[3]) : 2181;
            SimpleKafkaBroker broker = new SimpleKafkaBroker(brokerId, host, port, zkPort);
            broker.start();
            Runtime.getRuntime().addShutdownHook(new Thread(broker::stop));
            System.out.println("SimpleKafka broker started. Press Ctrl+C to stop.");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to start broker", e);
        }
    }
    
    public void start() throws IOException {
        if (isRunning.compareAndSet(false, true)) {
            serverChannel.socket().bind(new InetSocketAddress(brokerHost, brokerPort));
            serverChannel.configureBlocking(false);
            LOGGER.info("SimpleKafka broker started on " + brokerHost + ":" + brokerPort);
            registerWithZookeeper();
            electController();
            loadTopics();
            executor.submit(this::acceptConnections);
        }
    }
    
    public void stop() {
        if (isRunning.compareAndSet(true, false)) {
            try {
                LOGGER.info("Stopping SimpleKafka broker...");
                serverChannel.close();
                for (List<Partition> partitions : topics.values()) {
                    for (Partition partition : partitions) {
                        partition.close();
                    }
                }
                executor.shutdown();
                executor.awaitTermination(5, TimeUnit.SECONDS);
                zkClient.close();
                LOGGER.info("SimpleKafka broker stopped");
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error stopping broker", e);
            }
        }
    }
    
    private void registerWithZookeeper() {
        try {
            zkClient.connect();
            String brokerPath = "/brokers/" + brokerId;
            String brokerData = brokerHost + ":" + brokerPort;
            zkClient.createEphemeralNode(brokerPath, brokerData);
            BrokerInfo selfInfo = new BrokerInfo(brokerId, brokerHost, brokerPort);
            clusterMetadata.put(brokerId, selfInfo);
            zkClient.watchChildren("/brokers", this::onBrokersChanged);
            LOGGER.info("Registered with ZooKeeper at " + zkClient.getConnectString());
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to register with ZooKeeper", e);
        }
    }
    
    private void onBrokersChanged(List<String> brokerIds) {
        LOGGER.info("Broker change detected. Current brokers: " + brokerIds);
        for (String id : brokerIds) {
            try {
                int bId = Integer.parseInt(id);
                if (!clusterMetadata.containsKey(bId)) {
                    String brokerData = zkClient.getData("/brokers/" + id);
                    String[] hostPort = brokerData.split(":");
                    BrokerInfo info = new BrokerInfo(bId, hostPort[0], Integer.parseInt(hostPort[1]));
                    clusterMetadata.put(bId, info);
                    LOGGER.info("Added broker: " + info);
                }
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Failed to process broker info", e);
            }
        }
        List<Integer> toRemove = new ArrayList<>();
        for (Integer bId : clusterMetadata.keySet()) {
            if (!brokerIds.contains(String.valueOf(bId))) {
                toRemove.add(bId);
            }
        }
        for (Integer bId : toRemove) {
            clusterMetadata.remove(bId);
            LOGGER.info("Removed broker: " + bId);
        }
        if (!brokerIds.contains(String.valueOf(brokerId)) && isController.get()) {
            isController.set(false);
            LOGGER.info("This broker is no longer in the cluster, giving up controller status");
        } else if (isController.get()) {
            rebalancePartitions();
        } else {
            electController();
        }
    }
    
    private void electController() {
        try {
            String controllerPath = "/controller";
            boolean nodeExists = zkClient.exists(controllerPath);
            if (nodeExists) {
                String existingData = zkClient.getData(controllerPath);
                if (existingData == null || existingData.trim().isEmpty()) {
                    zkClient.deleteNode(controllerPath);
                    nodeExists = false;
                }
            }
            boolean becameController = false;
            if (!nodeExists) {
                becameController = zkClient.createEphemeralNode(controllerPath, String.valueOf(brokerId));
            }
            if (becameController) {
                isController.set(true);
                LOGGER.info("This broker is now the active controller");
                rebalancePartitions();
            } else {
                String controllerId = zkClient.getData(controllerPath);
                if (controllerId == null || controllerId.trim().isEmpty()) {
                    new Thread(() -> {
                        try {
                            Thread.sleep(1000);
                            electController();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }).start();
                    return;
                }
                LOGGER.info("Current controller is broker " + controllerId);
                zkClient.watchNode(controllerPath, this::onControllerChange);
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Controller election failed", e);
            new Thread(() -> {
                try {
                    Thread.sleep(2000);
                    electController();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }).start();
        }
    }
    
    private void onControllerChange() {
        LOGGER.info("Controller changed, initiating new election");
        electController();
    }
    
    private void rebalancePartitions() {
        if (!isController.get()) return;
        LOGGER.info("Rebalancing partitions across cluster");
        for (Map.Entry<String, List<Partition>> entry : topics.entrySet()) {
            String topic = entry.getKey();
            List<Partition> partitions = entry.getValue();
            for (Partition partition : partitions) {
                if (partition.getLeader() == -1 || !clusterMetadata.containsKey(partition.getLeader())) {
                    List<Integer> brokers = new ArrayList<>(clusterMetadata.keySet());
                    if (!brokers.isEmpty()) {
                        int newLeader = brokers.get(0);
                        partition.setLeader(newLeader);
                        List<Integer> followers = new ArrayList<>();
                        for (int i = 1; i < Math.min(brokers.size(), 3); i++) {
                            followers.add(brokers.get(i));
                        }
                        partition.setFollowers(followers);
                        updatePartitionMetadata(topic, partition);
                        LOGGER.info("Reassigned partition " + partition.getId() + " of topic " + topic + " to leader " + newLeader);
                    }
                }
            }
        }
    }
    
    private void updatePartitionMetadata(String topic, Partition partition) {
        try {
            String path = "/topics/" + topic + "/partitions/" + partition.getId();
            StringBuilder data = new StringBuilder(partition.getLeader() + ";");
            for (int follower : partition.getFollowers()) {
                data.append(follower).append(",");
            }
            if (zkClient.exists(path)) {
                zkClient.setData(path, data.toString());
            } else {
                zkClient.createPersistentNode(path, data.toString());
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to update partition metadata", e);
        }
    }
    
    private void loadTopics() {
        try {
            List<String> topicNames = zkClient.getChildren("/topics");
            for (String topic : topicNames) {
                try {
                    loadTopic(topic);
                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE, "Failed to load topic: " + topic, e);
                }
            }
            LOGGER.info("Loaded " + topics.size() + " topics");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to load topics", e);
        }
    }
    
    private void loadTopic(String topic) throws Exception {
        if (topics.containsKey(topic)) {
            return;
        }
        String topicPath = "/topics/" + topic;
        if (!zkClient.exists(topicPath)) {
            throw new Exception("Topic does not exist in ZooKeeper: " + topic);
        }
        String topicDir = DATA_DIR + File.separator + brokerId + File.separator + topic;
        new File(topicDir).mkdirs();
        List<String> partitionIds = zkClient.getChildren(topicPath + "/partitions");
        List<Partition> partitions = new ArrayList<>();
        for (String partitionId : partitionIds) {
            int id = Integer.parseInt(partitionId);
            String partitionPath = topicPath + "/partitions/" + partitionId;
            String partitionData = zkClient.getData(partitionPath);
            String[] parts = partitionData.split(";");
            int leader = Integer.parseInt(parts[0]);
            List<Integer> followers = new ArrayList<>();
            if (parts.length > 1 && !parts[1].isEmpty()) {
                String[] followerIds = parts[1].split(",");
                for (String followerId : followerIds) {
                    if (!followerId.isEmpty()) {
                        followers.add(Integer.parseInt(followerId));
                    }
                }
            }
            String partitionDir = topicDir + File.separator + id;
            new File(partitionDir).mkdirs();
            Partition partition = new Partition(id, leader, followers, partitionDir);
            partitions.add(partition);
        }
        topics.put(topic, partitions);
        LOGGER.info("Successfully loaded topic: " + topic + " with " + partitions.size() + " partitions");
    }
    
    private void acceptConnections() {
        while (isRunning.get()) {
            try {
                SocketChannel clientChannel = serverChannel.accept();
                if (clientChannel != null) {
                    clientChannel.configureBlocking(false);
                    executor.submit(() -> handleClient(clientChannel));
                }
                Thread.sleep(100);
            } catch (Exception e) {
                if (isRunning.get()) {
                    LOGGER.log(Level.SEVERE, "Error accepting connection", e);
                }
            }
        }
    }
    
    private void handleClient(SocketChannel clientChannel) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(1024);
            while (clientChannel.isOpen() && isRunning.get()) {
                buffer.clear();
                int bytesRead = clientChannel.read(buffer);
                if (bytesRead > 0) {
                    buffer.flip();
                    processClientMessage(clientChannel, buffer);
                } else if (bytesRead < 0) {
                    clientChannel.close();
                    break;
                }
                Thread.sleep(50);
            }
        } catch (Exception e) {
            if (isRunning.get()) {
                LOGGER.log(Level.SEVERE, "Error handling client", e);
            }
        } finally {
            try {
                if (clientChannel.isOpen()) {
                    clientChannel.close();
                }
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Error closing client channel", e);
            }
        }
    }
    
    private void processClientMessage(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        byte messageType = buffer.get();
        switch (messageType) {
            case Protocol.PRODUCE: handleProduceRequest(clientChannel, buffer); break;
            case Protocol.FETCH: handleFetchRequest(clientChannel, buffer); break;
            case Protocol.METADATA: handleMetadataRequest(clientChannel, buffer); break;
            case Protocol.CREATE_TOPIC: handleCreateTopicRequest(clientChannel, buffer); break;
            case Protocol.REPLICATE: handleReplicateRequest(clientChannel, buffer); break;
            case Protocol.TOPIC_NOTIFICATION: handleTopicNotification(clientChannel, buffer); break;
            default:
                LOGGER.warning("Unknown message type: " + messageType);
                Protocol.sendErrorResponse(clientChannel, "Unknown message type");
                break;
        }
    }
    
    private void handleProduceRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        short topicLength = buffer.getShort();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);
        int partition = buffer.getInt();
        int messageSize = buffer.getInt();
        byte[] message = new byte[messageSize];
        buffer.get(message);
        
        if (!topics.containsKey(topic)) {
            Protocol.sendErrorResponse(clientChannel, "Topic does not exist");
            return;
        }
        List<Partition> partitions = topics.get(topic);
        Partition targetPartition = null;
        for (Partition p : partitions) {
            if (p.getId() == partition) {
                targetPartition = p;
                break;
            }
        }
        if (targetPartition == null) {
            Protocol.sendErrorResponse(clientChannel, "Partition does not exist");
            return;
        }
        if (targetPartition.getLeader() != brokerId) {
            forwardProduceToLeader(clientChannel, topic, partition, message, targetPartition.getLeader());
            return;
        }
        long offset = targetPartition.append(message);
        replicateToFollowers(topic, targetPartition, message, offset);
        ByteBuffer response = ByteBuffer.allocate(10);
        response.put(Protocol.PRODUCE_RESPONSE);
        response.putLong(offset);
        response.put((byte) (offset > -1 ? 0 : 1));
        response.flip();
        clientChannel.write(response);
    }
    
    private void forwardProduceToLeader(SocketChannel clientChannel, String topic, int partition, byte[] message, int leaderId) throws IOException {
        BrokerInfo leader = clusterMetadata.get(leaderId);
        if (leader == null) {
            Protocol.sendErrorResponse(clientChannel, "Leader broker not available");
            return;
        }
        try (SocketChannel leaderChannel = SocketChannel.open()) {
            leaderChannel.connect(new InetSocketAddress(leader.getHost(), leader.getPort()));
            ByteBuffer request = ByteBuffer.allocate(9 + topic.length() + message.length);
            request.put(Protocol.PRODUCE);
            request.putShort((short) topic.length());
            request.put(topic.getBytes());
            request.putInt(partition);
            request.putInt(message.length);
            request.put(message);
            request.flip();
            leaderChannel.write(request);
            ByteBuffer response = ByteBuffer.allocate(10);
            leaderChannel.read(response);
            response.flip();
            clientChannel.write(response);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to forward produce request to leader", e);
            Protocol.sendErrorResponse(clientChannel, "Failed to forward to leader");
        }
    }
    
    private void replicateToFollowers(String topic, Partition partition, byte[] message, long offset) {
        for (int followerId : partition.getFollowers()) {
            if (followerId == brokerId) continue;
            BrokerInfo follower = clusterMetadata.get(followerId);
            if (follower == null) continue;
            executor.submit(() -> {
                try (SocketChannel followerChannel = SocketChannel.open()) {
                    followerChannel.connect(new InetSocketAddress(follower.getHost(), follower.getPort()));
                    ByteBuffer request = ByteBuffer.allocate(17 + topic.length() + message.length);
                    request.put(Protocol.REPLICATE);
                    request.putShort((short) topic.length());
                    request.put(topic.getBytes());
                    request.putInt(partition.getId());
                    request.putLong(offset);
                    request.putInt(message.length);
                    request.put(message);
                    request.flip();
                    followerChannel.write(request);
                    ByteBuffer response = ByteBuffer.allocate(1);
                    followerChannel.read(response);
                    response.flip();
                    byte ack = response.get();
                    LOGGER.info("Replication to follower " + followerId + " " + (ack == Protocol.REPLICATE_ACK ? "succeeded" : "failed"));
                } catch (IOException e) {
                    LOGGER.log(Level.SEVERE, "Replication to follower " + followerId + " failed", e);
                }
            });
        }
    }
    
    private void handleReplicateRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        short topicLength = buffer.getShort();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);
        int partitionId = buffer.getInt();
        long offset = buffer.getLong();
        int messageSize = buffer.getInt();
        byte[] message = new byte[messageSize];
        buffer.get(message);
        if (!topics.containsKey(topic)) {
            ByteBuffer response = ByteBuffer.allocate(1);
            response.put((byte) 0);
            response.flip();
            clientChannel.write(response);
            return;
        }
        List<Partition> partitions = topics.get(topic);
        Partition targetPartition = null;
        for (Partition p : partitions) {
            if (p.getId() == partitionId) {
                targetPartition = p;
                break;
            }
        }
        if (targetPartition == null) {
            ByteBuffer response = ByteBuffer.allocate(1);
            response.put((byte) 0);
            response.flip();
            clientChannel.write(response);
            return;
        }
        targetPartition.append(message);
        ByteBuffer response = ByteBuffer.allocate(1);
        response.put(Protocol.REPLICATE_ACK);
        response.flip();
        clientChannel.write(response);
    }
    
    private void handleFetchRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        short topicLength = buffer.getShort();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);
        int partition = buffer.getInt();
        long offset = buffer.getLong();
        int maxBytes = buffer.getInt();
        if (!topics.containsKey(topic)) {
            Protocol.sendErrorResponse(clientChannel, "Topic does not exist");
            return;
        }
        List<Partition> partitions = topics.get(topic);
        Partition targetPartition = null;
        for (Partition p : partitions) {
            if (p.getId() == partition) {
                targetPartition = p;
                break;
            }
        }
        if (targetPartition == null) {
            Protocol.sendErrorResponse(clientChannel, "Partition does not exist");
            return;
        }
        if (offset >= targetPartition.getLogEndOffset()) {
            ByteBuffer response = ByteBuffer.allocate(5);
            response.put(Protocol.FETCH_RESPONSE);
            response.putInt(0);
            response.flip();
            clientChannel.write(response);
            return;
        }
        List<byte[]> messages = targetPartition.readMessages(offset, maxBytes);
        int totalSize = 5;
        for (byte[] msg : messages) {
            totalSize += 12 + msg.length;
        }
        ByteBuffer response = ByteBuffer.allocate(totalSize);
        response.put(Protocol.FETCH_RESPONSE);
        response.putInt(messages.size());
        long currentOffset = offset;
        for (byte[] msg : messages) {
            response.putLong(currentOffset);
            response.putInt(msg.length);
            response.put(msg);
            currentOffset++;
        }
        response.flip();
        clientChannel.write(response);
    }
    
    private void handleMetadataRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        int size = 5;
        for (Map.Entry<String, List<Partition>> entry : topics.entrySet()) {
            size += 6 + entry.getKey().length();
            size += entry.getValue().size() * 12;
            for (Partition partition : entry.getValue()) {
                size += partition.getFollowers().size() * 4;
            }
        }
        size += 4 + clusterMetadata.size() * 10;
        for (BrokerInfo broker : clusterMetadata.values()) {
            size += broker.getHost().length();
        }
        ByteBuffer response = ByteBuffer.allocate(size);
        response.put(Protocol.METADATA_RESPONSE);
        response.putInt(clusterMetadata.size());
        for (BrokerInfo broker : clusterMetadata.values()) {
            response.putInt(broker.getId());
            response.putShort((short) broker.getHost().length());
            response.put(broker.getHost().getBytes());
            response.putInt(broker.getPort());
        }
        response.putInt(topics.size());
        for (Map.Entry<String, List<Partition>> entry : topics.entrySet()) {
            String topic = entry.getKey();
            List<Partition> partitions = entry.getValue();
            response.putShort((short) topic.length());
            response.put(topic.getBytes());
            response.putInt(partitions.size());
            for (Partition partition : partitions) {
                response.putInt(partition.getId());
                response.putInt(partition.getLeader());
                List<Integer> followers = partition.getFollowers();
                response.putInt(followers.size());
                for (Integer follower : followers) {
                    response.putInt(follower);
                }
            }
        }
        response.flip();
        clientChannel.write(response);
    }
    
    private void handleCreateTopicRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        short topicLength = buffer.getShort();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);
        int numPartitions = buffer.getInt();
        short replicationFactor = buffer.getShort();
        if (topics.containsKey(topic)) {
            Protocol.sendErrorResponse(clientChannel, "Topic already exists");
            return;
        }
        if (numPartitions <= 0 || replicationFactor <= 0 || replicationFactor > clusterMetadata.size()) {
            Protocol.sendErrorResponse(clientChannel, "Invalid topic configuration");
            return;
        }
        if (isController.get()) {
            createTopic(topic, numPartitions, replicationFactor);
            ByteBuffer response = ByteBuffer.allocate(2);
            response.put(Protocol.CREATE_TOPIC_RESPONSE);
            response.put((byte) 0);
            response.flip();
            clientChannel.write(response);
        } else {
            forwardCreateTopicToController(clientChannel, topic, numPartitions, replicationFactor);
        }
    }
    
    private void forwardCreateTopicToController(SocketChannel clientChannel, String topic, int numPartitions, short replicationFactor) throws IOException {
        int controllerId = -1;
        try {
            String controllerData = zkClient.getData("/controller");
            controllerId = Integer.parseInt(controllerData);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to get controller info", e);
            Protocol.sendErrorResponse(clientChannel, "Controller not available");
            return;
        }
        BrokerInfo controller = clusterMetadata.get(controllerId);
        if (controller == null) {
            Protocol.sendErrorResponse(clientChannel, "Controller broker not available");
            return;
        }
        try (SocketChannel controllerChannel = SocketChannel.open()) {
            controllerChannel.connect(new InetSocketAddress(controller.getHost(), controller.getPort()));
            ByteBuffer request = ByteBuffer.allocate(9 + topic.length());
            request.put(Protocol.CREATE_TOPIC);
            request.putShort((short) topic.length());
            request.put(topic.getBytes());
            request.putInt(numPartitions);
            request.putShort(replicationFactor);
            request.flip();
            controllerChannel.write(request);
            ByteBuffer response = ByteBuffer.allocate(2);
            controllerChannel.read(response);
            response.flip();
            clientChannel.write(response);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to forward create topic request to controller", e);
            Protocol.sendErrorResponse(clientChannel, "Failed to forward to controller");
        }
    }
    
    private void createTopic(String topic, int numPartitions, short replicationFactor) {
        if (!isController.get()) return;
        try {
            String topicDir = DATA_DIR + File.separator + brokerId + File.separator + topic;
            new File(topicDir).mkdirs();
            String topicPath = "/topics/" + topic;
            if (!zkClient.exists(topicPath)) {
                zkClient.createPersistentNode(topicPath, "");
                zkClient.createPersistentNode(topicPath + "/partitions", "");
            }
            List<Partition> partitions = new ArrayList<>();
            List<Integer> brokerIds = new ArrayList<>(clusterMetadata.keySet());
            for (int i = 0; i < numPartitions; i++) {
                int partitionId = i;
                String partitionDir = topicDir + File.separator + partitionId;
                new File(partitionDir).mkdirs();
                int leaderIndex = i % brokerIds.size();
                int leaderId = brokerIds.get(leaderIndex);
                List<Integer> followers = new ArrayList<>();
                for (int j = 1; j < replicationFactor; j++) {
                    int followerIndex = (leaderIndex + j) % brokerIds.size();
                    followers.add(brokerIds.get(followerIndex));
                }
                Partition partition = new Partition(partitionId, leaderId, followers, partitionDir);
                partitions.add(partition);
                String partitionPath = topicPath + "/partitions/" + partitionId;
                StringBuilder partitionData = new StringBuilder(leaderId + ";");
                for (int follower : followers) {
                    partitionData.append(follower).append(",");
                }
                zkClient.createPersistentNode(partitionPath, partitionData.toString());
            }
            topics.put(topic, partitions);
            for (int bId : brokerIds) {
                if (bId != this.brokerId) {
                    notifyBrokerForTopicCreation(bId, topic);
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to create topic", e);
        }
    }
    
    private void notifyBrokerForTopicCreation(int brokerId, String topic) {
        BrokerInfo broker = clusterMetadata.get(brokerId);
        if (broker == null) return;
        executor.submit(() -> {
            try (SocketChannel brokerChannel = SocketChannel.open()) {
                brokerChannel.connect(new InetSocketAddress(broker.getHost(), broker.getPort()));
                ByteBuffer request = ByteBuffer.allocate(3 + topic.length());
                request.put(Protocol.TOPIC_NOTIFICATION);
                request.putShort((short) topic.length());
                request.put(topic.getBytes());
                request.flip();
                brokerChannel.write(request);
                ByteBuffer response = ByteBuffer.allocate(1);
                brokerChannel.read(response);
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to notify broker " + brokerId + " about topic creation", e);
            }
        });
    }
    
    private void handleTopicNotification(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        short topicLength = buffer.getShort();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);
        try {
            loadTopic(topic);
            ByteBuffer response = ByteBuffer.allocate(1);
            response.put((byte) 0);
            response.flip();
            clientChannel.write(response);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to load topic: " + topic, e);
            ByteBuffer response = ByteBuffer.allocate(1);
            response.put((byte) 1);
            response.flip();
            clientChannel.write(response);
        }
    }
}

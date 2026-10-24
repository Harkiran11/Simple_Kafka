package simplekafka.client;

import simplekafka.broker.Protocol;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class SimpleKafkaAdmin {
    public static void main(String[] args) {
        if (args.length < 5) {
            System.out.println("Usage: SimpleKafkaAdmin <brokerHost> <brokerPort> <topic> <numPartitions> <replicationFactor>");
            System.exit(1);
        }
        try {
            String host = args[0];
            int port = Integer.parseInt(args[1]);
            String topic = args[2];
            int numPartitions = Integer.parseInt(args[3]);
            short replicationFactor = Short.parseShort(args[4]);
            
            try (SocketChannel channel = SocketChannel.open()) {
                channel.connect(new InetSocketAddress(host, port));
                ByteBuffer request = Protocol.encodeCreateTopicRequest(topic, numPartitions, replicationFactor);
                channel.write(request);
                
                ByteBuffer response = ByteBuffer.allocate(2);
                channel.read(response);
                response.flip();
                byte responseType = response.get();
                if (responseType == Protocol.CREATE_TOPIC_RESPONSE) {
                    byte status = response.get();
                    if (status == 0) {
                        System.out.println("Topic created successfully: " + topic);
                    } else {
                        System.out.println("Failed to create topic: " + topic);
                    }
                } else if (responseType == Protocol.ERROR_RESPONSE) {
                    short errorLen = response.getShort();
                    byte[] errorBytes = new byte[errorLen];
                    response.get(errorBytes);
                    System.out.println("Error: " + new String(errorBytes));
                } else {
                    System.out.println("Unexpected response type: " + responseType);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

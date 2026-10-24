package simplekafka.broker;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

public class ZookeeperClient {
    private ZooKeeper zooKeeper;
    private final String connectString;
    private final CountDownLatch connectedSignal = new CountDownLatch(1);
    
    public ZookeeperClient(String host, int port) {
        this.connectString = host + ":" + port;
    }
    
    public void connect() throws IOException, InterruptedException {
        zooKeeper = new ZooKeeper(connectString, 30000, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connectedSignal.countDown();
            }
        });
        connectedSignal.await();
    }
    
    public void createPath(String path) throws Exception {
        String[] parts = path.split("/");
        StringBuilder currentPath = new StringBuilder();
        for (int i = 1; i < parts.length; i++) {
            currentPath.append("/").append(parts[i]);
            String pathStr = currentPath.toString();
            if (zooKeeper.exists(pathStr, false) == null) {
                zooKeeper.create(pathStr, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        }
    }
    
    public boolean createEphemeralNode(String path, String data) throws Exception {
        try {
            if (zooKeeper.exists(path, false) != null) {
                zooKeeper.delete(path, -1);
            }
            zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
    
    public void createPersistentNode(String path, String data) throws Exception {
        if (zooKeeper.exists(path, false) == null) {
            zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }
    
    public String getData(String path) throws Exception {
        Stat stat = new Stat();
        byte[] data = zooKeeper.getData(path, false, stat);
        return data.length == 0 ? "" : new String(data);
    }
    
    public void setData(String path, String data) throws Exception {
        zooKeeper.setData(path, data.getBytes(), -1);
    }
    
    public List<String> getChildren(String path) throws Exception {
        return zooKeeper.getChildren(path, false);
    }
    
    public void watchChildren(String path, Consumer<List<String>> callback) throws Exception {
        zooKeeper.getChildren(path, event -> {
            if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                try {
                    callback.accept(zooKeeper.getChildren(path, this::wrapChildrenWatch, null));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }
    
    private void wrapChildrenWatch(Watcher event) {
        if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
            try {
                // Re-watch will be handled by caller
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    
    public void watchNode(String path, Runnable callback) throws Exception {
        zooKeeper.exists(path, event -> {
            if (event.getType() == Watcher.Event.EventType.NodeDeleted || 
                event.getType() == Watcher.Event.EventType.NodeCreated) {
                callback.run();
            }
        });
    }
    
    public boolean exists(String path) throws Exception {
        return zooKeeper.exists(path, false) != null;
    }
    
    public void deleteNode(String path) throws Exception {
        if (zooKeeper.exists(path, false) != null) {
            zooKeeper.delete(path, -1);
        }
    }
    
    public String getConnectString() {
        return connectString;
    }
    
    public void close() throws InterruptedException {
        if (zooKeeper != null) {
            zooKeeper.close();
        }
    }
}

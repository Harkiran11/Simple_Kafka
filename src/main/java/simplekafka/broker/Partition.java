package simplekafka.broker;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Partition {
    private static final Logger LOGGER = Logger.getLogger(Partition.class.getName());
    private static final int DEFAULT_SEGMENT_SIZE = 1024 * 1024;
    private static final String LOG_SUFFIX = ".log";
    private static final String INDEX_SUFFIX = ".index";
    
    private final int id;
    private int leader;
    private List<Integer> followers;
    private final String baseDir;
    private final AtomicLong nextOffset;
    private final ReadWriteLock lock;
    private RandomAccessFile activeLogFile;
    private FileChannel activeLogChannel;
    private final List<SegmentInfo> segments;
    
    public Partition(int id, int leader, List<Integer> followers, String baseDir) {
        this.id = id;
        this.leader = leader;
        this.followers = followers;
        this.baseDir = baseDir;
        this.nextOffset = new AtomicLong(0);
        this.lock = new ReentrantReadWriteLock();
        this.segments = new ArrayList<>();
        initialize();
    }
    
    private void initialize() {
        try {
            File dir = new File(baseDir);
            if (!dir.exists()) {
                dir.mkdirs();
            }
            File[] files = dir.listFiles((dir1, name) -> name.endsWith(LOG_SUFFIX));
            if (files != null && files.length > 0) {
                for (File file : files) {
                    String baseName = file.getName().substring(0, file.getName().length() - LOG_SUFFIX.length());
                    long baseOffset = Long.parseLong(baseName);
                    File indexFile = new File(baseDir, baseName + INDEX_SUFFIX);
                    if (indexFile.exists()) {
                        SegmentInfo segment = new SegmentInfo(baseOffset, file.getAbsolutePath(), indexFile.getAbsolutePath());
                        segments.add(segment);
                    }
                }
                segments.sort((s1, s2) -> Long.compare(s1.getBaseOffset(), s2.getBaseOffset()));
                if (!segments.isEmpty()) {
                    SegmentInfo lastSegment = segments.get(segments.size() - 1);
                    nextOffset.set(lastSegment.getBaseOffset() + countMessagesInSegment(lastSegment));
                }
            }
            if (segments.isEmpty()) {
                createNewSegment(0);
            } else {
                SegmentInfo lastSegment = segments.get(segments.size() - 1);
                openSegmentForAppend(lastSegment);
            }
            LOGGER.info("Initialized partition " + id + " with " + segments.size() + " segments, next offset: " + nextOffset.get());
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to initialize partition " + id, e);
        }
    }
    
    private long countMessagesInSegment(SegmentInfo segment) throws IOException {
        long count = 0;
        try (RandomAccessFile logFile = new RandomAccessFile(segment.getLogPath(), "r");
             FileChannel logChannel = logFile.getChannel()) {
            ByteBuffer buffer = ByteBuffer.allocate(4);
            while (logChannel.position() < logChannel.size()) {
                buffer.clear();
                int bytesRead = logChannel.read(buffer);
                if (bytesRead < 4) break;
                buffer.flip();
                int messageSize = buffer.getInt();
                logChannel.position(logChannel.position() + messageSize);
                count++;
            }
        }
        return count;
    }
    
    private void createNewSegment(long baseOffset) throws IOException {
        String baseName = String.format("%020d", baseOffset);
        String logPath = baseDir + File.separator + baseName + LOG_SUFFIX;
        String indexPath = baseDir + File.separator + baseName + INDEX_SUFFIX;
        File logFile = new File(logPath);
        logFile.createNewFile();
        File indexFile = new File(indexPath);
        indexFile.createNewFile();
        SegmentInfo segment = new SegmentInfo(baseOffset, logPath, indexPath);
        segments.add(segment);
        openSegmentForAppend(segment);
        LOGGER.info("Created new segment for partition " + id + ", base offset: " + baseOffset);
    }
    
    private void openSegmentForAppend(SegmentInfo segment) throws IOException {
        if (activeLogChannel != null && activeLogChannel.isOpen()) {
            activeLogChannel.close();
        }
        if (activeLogFile != null) {
            activeLogFile.close();
        }
        activeLogFile = new RandomAccessFile(segment.getLogPath(), "rw");
        activeLogChannel = activeLogFile.getChannel();
        activeLogChannel.position(activeLogChannel.size());
    }
    
    public long append(byte[] message) {
        lock.writeLock().lock();
        try {
            long currentOffset = nextOffset.get();
            if (activeLogChannel.position() >= DEFAULT_SEGMENT_SIZE) {
                activeLogChannel.close();
                activeLogFile.close();
                createNewSegment(currentOffset);
            }
            ByteBuffer buffer = ByteBuffer.allocate(4 + message.length);
            buffer.putInt(message.length);
            buffer.put(message);
            buffer.flip();
            long position = activeLogChannel.position();
            activeLogChannel.write(buffer);
            activeLogChannel.force(true);
            updateIndex(currentOffset, position);
            nextOffset.incrementAndGet();
            return currentOffset;
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to append message to partition " + id, e);
            return -1;
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    private void updateIndex(long offset, long position) {
        try {
            if (segments.isEmpty()) return;
            SegmentInfo currentSegment = segments.get(segments.size() - 1);
            try (RandomAccessFile indexFile = new RandomAccessFile(currentSegment.getIndexPath(), "rw");
                 FileChannel indexChannel = indexFile.getChannel()) {
                indexChannel.position(indexChannel.size());
                ByteBuffer buffer = ByteBuffer.allocate(16);
                buffer.putLong(offset);
                buffer.putLong(position);
                buffer.flip();
                indexChannel.write(buffer);
                indexChannel.force(true);
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to update index for partition " + id, e);
        }
    }
    
    public List<byte[]> readMessages(long offset, int maxBytes) {
        lock.readLock().lock();
        List<byte[]> messages = new ArrayList<>();
        int bytesRead = 0;
        try {
            SegmentInfo targetSegment = findSegmentForOffset(offset);
            if (targetSegment == null) {
                return messages;
            }
            long position = findPositionForOffset(targetSegment, offset);
            if (position < 0) {
                return messages;
            }
            try (RandomAccessFile logFile = new RandomAccessFile(targetSegment.getLogPath(), "r");
                 FileChannel logChannel = logFile.getChannel()) {
                logChannel.position(position);
                ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
                long currentOffset = offset;
                while (bytesRead < maxBytes && logChannel.position() < logChannel.size()) {
                    sizeBuffer.clear();
                    int sizeRead = logChannel.read(sizeBuffer);
                    if (sizeRead < 4) break;
                    sizeBuffer.flip();
                    int messageSize = sizeBuffer.getInt();
                    if (bytesRead + messageSize > maxBytes) {
                        break;
                    }
                    ByteBuffer messageBuffer = ByteBuffer.allocate(messageSize);
                    int messageRead = logChannel.read(messageBuffer);
                    if (messageRead < messageSize) {
                        break;
                    }
                    messageBuffer.flip();
                    byte[] message = new byte[messageSize];
                    messageBuffer.get(message);
                    messages.add(message);
                    bytesRead += messageSize + 4;
                    currentOffset++;
                    if (logChannel.position() >= logChannel.size() && currentOffset < nextOffset.get()) {
                        int nextSegmentIndex = segments.indexOf(targetSegment) + 1;
                        if (nextSegmentIndex < segments.size()) {
                            logChannel.close();
                            logFile.close();
                            targetSegment = segments.get(nextSegmentIndex);
                            RandomAccessFile nextLogFile = new RandomAccessFile(targetSegment.getLogPath(), "r");
                            FileChannel nextLogChannel = nextLogFile.getChannel();
                            position = 0;
                            nextLogChannel.position(position);
                        }
                    }
                }
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to read messages from partition " + id, e);
        } finally {
            lock.readLock().unlock();
        }
        return messages;
    }
    
    private SegmentInfo findSegmentForOffset(long offset) {
        if (segments.isEmpty() || offset >= nextOffset.get()) {
            return null;
        }
        int low = 0;
        int high = segments.size() - 1;
        while (low <= high) {
            int mid = (low + high) / 2;
            SegmentInfo segment = segments.get(mid);
            if (mid < segments.size() - 1) {
                SegmentInfo nextSegment = segments.get(mid + 1);
                if (offset >= segment.getBaseOffset() && offset < nextSegment.getBaseOffset()) {
                    return segment;
                }
            } else {
                if (offset >= segment.getBaseOffset()) {
                    return segment;
                }
            }
            if (offset < segment.getBaseOffset()) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return null;
    }
    
    private long findPositionForOffset(SegmentInfo segment, long offset) {
        try (RandomAccessFile indexFile = new RandomAccessFile(segment.getIndexPath(), "r");
             FileChannel indexChannel = indexFile.getChannel()) {
            if (indexChannel.size() == 0) {
                return 0;
            }
            long relativeOffset = offset - segment.getBaseOffset();
            long entryCount = indexChannel.size() / 16;
            if (relativeOffset >= entryCount) {
                indexChannel.position(indexChannel.size() - 16);
                ByteBuffer buffer = ByteBuffer.allocate(16);
                indexChannel.read(buffer);
                buffer.flip();
                buffer.getLong();
                return buffer.getLong();
            }
            indexChannel.position(relativeOffset * 16);
            ByteBuffer buffer = ByteBuffer.allocate(16);
            indexChannel.read(buffer);
            buffer.flip();
            buffer.getLong();
            return buffer.getLong();
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to find position for offset " + offset, e);
            return -1;
        }
    }
    
    public int getId() { return id; }
    public int getLeader() { return leader; }
    public void setLeader(int leader) { this.leader = leader; }
    public List<Integer> getFollowers() { return new ArrayList<>(followers); }
    public void setFollowers(List<Integer> followers) { this.followers = new ArrayList<>(followers); }
    public long getLogEndOffset() { return nextOffset.get(); }
    
    public void close() {
        lock.writeLock().lock();
        try {
            if (activeLogChannel != null && activeLogChannel.isOpen()) {
                activeLogChannel.close();
            }
            if (activeLogFile != null) {
                activeLogFile.close();
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to close partition resources", e);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    private static class SegmentInfo {
        private final long baseOffset;
        private final String logPath;
        private final String indexPath;
        public SegmentInfo(long baseOffset, String logPath, String indexPath) {
            this.baseOffset = baseOffset;
            this.logPath = logPath;
            this.indexPath = indexPath;
        }
        public long getBaseOffset() { return baseOffset; }
        public String getLogPath() { return logPath; }
        public String getIndexPath() { return indexPath; }
    }
}

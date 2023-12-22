package diskbased.asyncio;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public class FileIOPubSub {
    private BlockingQueue<Integer> queue;
    private boolean isActive = true;

    public FileIOPubSub(int queueSize, int bufferSize, String file) throws IOException {
        this.queue = new ArrayBlockingQueue<>(queueSize);
        new Thread(new AsyncFileReader(queue, file, bufferSize)).start();
    }

    // Method to test async IO consumer code separately
    public static void main(String[] args) throws IOException, InterruptedException {
        String fileName = "./src/main/java/diskbased/asyncio/sample.txt";
        int maxNumber = 10000000;

        // Write data
        BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
        for (int i = 0; i < maxNumber; i++) {
            writer.write(i + "\n");
        }
        writer.close();

        // Async IO
        long start = System.currentTimeMillis();
        FileIOPubSub pubSub = new FileIOPubSub(5 * maxNumber, maxNumber, "./src/main/java/diskbased/asyncio/sample.txt");
        List<Integer> ints = pubSub.readInts(maxNumber);
        validateInts(maxNumber, ints);
        long end = System.currentTimeMillis();
        System.out.println("Passed. Took " + (end - start) + " millis");

        // Blocking IO
        ints.clear();
        start = System.currentTimeMillis();
        BufferedReader reader = new BufferedReader(new java.io.FileReader(fileName));
        String line;
        while ((line = reader.readLine()) != null) {
            ints.add(Integer.parseInt(line));
        }
        validateInts(maxNumber, ints);
        end = System.currentTimeMillis();
        System.out.println("Passed. Took " + (end - start) + " millis");
        System.exit(0);
    }

    private static void validateInts(int maxNumber, List<Integer> ints) {
        if (ints.size() != maxNumber) {
            System.out.println("Failed. Size=" + ints.get(ints.size() - 1));
            System.exit(1);
        }
        for (int i = 0; i < ints.size(); i++) {
            if (i != ints.get(i)) {
                System.out.println("Failed");
                System.exit(1);
            }
        }
    }

    public List<Integer> readInts(int size) throws InterruptedException {
        int count = 0;
        List<Integer> list = new ArrayList<>();
        if (!isActive) {
            return list;
        }
        while (count < size) {
            int num = queue.take();
            if (num == -1) {
                isActive = false;
                break;
            }
            list.add(num);
            count++;
        }
        return list;
    }

    public static class AsyncFileReader implements Runnable {
        private final BlockingQueue<Integer> queue;
        private AsynchronousFileChannel channel;
        private ByteBuffer buffer;
        private int currNo = 0;
        private long pos = 0;

        public AsyncFileReader(final BlockingQueue<Integer> queue, String file, int bufferSize) throws IOException {
            this.queue = queue;
            this.channel = AsynchronousFileChannel.open(Path.of(file));
            this.buffer = ByteBuffer.allocate(bufferSize);
        }

        @Override
        public void run() {
            try {
                AtomicBoolean readMore = new AtomicBoolean(true);
                while (readMore.get()) {
                    CompletableFuture.supplyAsync(() -> readBytes(pos)).exceptionally(ex -> {
                        ex.printStackTrace();
                        readMore.set(false);
                        return null;
                    }).thenApply(numBytes -> {
                        try {
                            if (numBytes > 0) {
                                buffer.flip();
                                this.currNo = convertToInt(buffer, this.currNo, queue);
                                this.pos += numBytes;
                                buffer.clear();
                                return numBytes;
                            } else {
                                readMore.set(false);
                                return -1;
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            readMore.set(false);
                            return -1;
                        }
                    }).join();
                }
                queue.put(-1);
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                if (channel != null && channel.isOpen()) {
                    try {
                        buffer.clear();
                        channel.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        private Integer readBytes(long pos) {
            try {
                return channel.read(buffer, pos).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            return -1;
        }

        private int convertToInt(ByteBuffer data, int initial, final BlockingQueue<Integer> queue) throws InterruptedException {
            int currNo = initial;
            while (data.hasRemaining()) {
                char ch = (char) (data.get() & 0xFF);
                if (ch == '\n') {
                    queue.put(currNo);
                    currNo = 0;
                } else {
                    currNo = (currNo * 10) + (ch - '0');
                }
            }
            return currNo;
        }
    }
}

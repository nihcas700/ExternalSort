package diskbased.asyncio;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class FileIOPubSub {
    final LinkedList<Integer> queue;
    final ReentrantLock queueLock = new ReentrantLock();
    final Condition produceMode = queueLock.newCondition();
    final Condition consumeMode = queueLock.newCondition();
    private boolean isActive = true;

    public FileIOPubSub(int queueSize, int bufferSize, String file) throws IOException {
        this.queue = new LinkedList<>();
        new Thread(new AsyncFileReader(file, bufferSize)).start();
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
        FileIOPubSub pubSub = new FileIOPubSub(maxNumber + 5, maxNumber/200, "./src/main/java/diskbased/asyncio/sample.txt");
        Iterator<Integer> pubSubIter = pubSub.iterator();
        List<Integer> ints = new ArrayList<>();
        while (pubSubIter.hasNext()) {
            ints.add(pubSubIter.next());
        }
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

    public List<Integer> readInts(int size) throws InterruptedException {
        int count = 0;
        List<Integer> list = new ArrayList<>();
        while (count < size) {
            int num = queue.get(0);
            if (num == -1) break;
            list.add(num);
            count++;
        }
        return list;
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

    public Iterator<Integer> iterator() {
        return new Iterator<>() {
            private int nextNo = -2;

            @Override
            public boolean hasNext() {
                if (nextNo < -1) {
                    fillNext();
                }
                return isActive;
            }

            private void fillNext() {
                queueLock.lock();
                while (queue.isEmpty()) {
                    try {
                        consumeMode.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                }
                nextNo = queue.poll();
                produceMode.signal();
                queueLock.unlock();
                if (nextNo == -1) {
                    isActive = false;
                }
            }

            @Override
            public Integer next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                int tmp = nextNo;
                fillNext();
                return tmp;
            }
        };
    }

    public class AsyncFileReader implements Runnable {
        private AsynchronousFileChannel channel;
        private ByteBuffer buffer;
        private int currNo = 0;
        private long pos = 0;
        private AtomicInteger iterationCount = new AtomicInteger(0);
        private AtomicLong cpuTime = new AtomicLong(0);
        private AtomicLong cpu2Time = new AtomicLong(0);
        private AtomicLong ioTime = new AtomicLong(0);

        public AsyncFileReader(String file, int bufferSize) throws IOException {
            this.channel = AsynchronousFileChannel.open(Path.of(file));
            this.buffer = ByteBuffer.allocate(bufferSize);
        }

        @Override
        public void run() {
            try {
                AtomicBoolean readMore = new AtomicBoolean(true);
                while (readMore.get()) {
                    queueLock.lock();
                    while (!queue.isEmpty()) {
                        produceMode.await();
                    }
                    iterationCount.getAndAdd(1);
                    CompletableFuture.supplyAsync(() -> readBytes(pos)).exceptionally(ex -> {
                        ex.printStackTrace();
                        readMore.set(false);
                        return null;
                    }).thenApply(numBytes -> {
                        try {
                            if (numBytes > 0) {
                                buffer.flip();
                                this.currNo = convertToInt(buffer, this.currNo);
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
                    consumeMode.signal();
                    queueLock.unlock();
                }

                System.out.println("Total Iterations:" + iterationCount.get());
                System.out.println("Total cpu time:" + cpuTime.get());
                System.out.println("Total cpu time:" + cpu2Time.get());
                System.out.println("Total io time:" + ioTime.get());
                queueLock.lock();
                queue.add(-1);
                consumeMode.signal();
                queueLock.unlock();
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
            long start = System.currentTimeMillis();
            try {
                return channel.read(buffer, pos).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            ioTime.getAndAdd(System.currentTimeMillis() - start);
            return -1;
        }

        private int convertToInt(ByteBuffer data, int initial) {
            long start = System.currentTimeMillis();
            int currNo = initial;
            while (data.hasRemaining()) {
                char ch = (char) (data.get() & 0xFF);
                if (ch == '\n') {
                    long startt = System.currentTimeMillis();
                    queue.add(currNo);
                    currNo = 0;
                    cpu2Time.getAndAdd(System.currentTimeMillis() - startt);
                } else {
                    currNo = (currNo * 10) + (ch - '0');
                }
            }
            cpuTime.getAndAdd(System.currentTimeMillis() - start);
            return currNo;
        }
    }
}

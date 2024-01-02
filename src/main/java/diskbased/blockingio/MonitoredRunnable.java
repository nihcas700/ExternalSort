package diskbased.blockingio;

import diskbased.ThreadMetadata;

public abstract class MonitoredRunnable implements Runnable {
    private ThreadMetadata metadata;
    private int linesProcessed = 0;

    public MonitoredRunnable(ThreadMetadata metadata) {
        this.metadata = metadata;
    }

    @Override
    public void run() {
        long start = System.currentTimeMillis();

        // Actual logic
        doRun();

        // Set thread metadata
        metadata.setRunTime(System.currentTimeMillis() - start);
        metadata.setLinesProcessed(linesProcessed);
    }

    public void incrementLinesProcessed() {
        linesProcessed++;
    }

    public abstract void doRun();
}

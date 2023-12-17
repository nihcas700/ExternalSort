package diskbased;

import java.util.concurrent.CompletableFuture;

public class ThreadMetadata {
    public CompletableFuture<?> getFuture() {
        return future;
    }

    public void setFuture(CompletableFuture<?> future) {
        this.future = future;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    private String fileName;

    public int getLinesProcessed() {
        return linesProcessed;
    }

    public void setLinesProcessed(int linesProcessed) {
        this.linesProcessed = linesProcessed;
    }

    private int linesProcessed;
    private CompletableFuture<?> future;
    public long getRunTime() {
        return runTime;
    }

    public void setRunTime(long runTime) {
        this.runTime = runTime;
    }

    private long runTime;
}

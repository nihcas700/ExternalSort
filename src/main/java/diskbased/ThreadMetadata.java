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

    public int getLinesProcessed() {
        return linesProcessed;
    }

    public void setLinesProcessed(int linesProcessed) {
        this.linesProcessed = linesProcessed;
    }

    public long getRunTime() {
        return runTime;
    }

    public void setRunTime(long runTime) {
        this.runTime = runTime;
    }

    public long getIoTime() {
        return ioTime;
    }

    public void setIoTime(long ioTime) {
        this.ioTime = ioTime;
    }

    public long getIoReadTime() {
        return ioReadTime;
    }

    public void setIoReadTime(long ioReadTime) {
        this.ioReadTime = ioReadTime;
    }

    public long getCpuTime() {
        return cpuTime;
    }

    public void setCpuTime(long cpuTime) {
        this.cpuTime = cpuTime;
    }

    public long getIoWriteTime() {
        return ioWriteTime;
    }

    public void setIoWriteTime(long ioWriteTime) {
        this.ioWriteTime = ioWriteTime;
    }

    private long ioWriteTime;
    private long ioReadTime;
    private long ioTime;
    private long cpuTime;
    private long runTime;
    private int linesProcessed;
    private CompletableFuture<?> future;
    private String fileName;
}

package diskbased.blockingio;

import diskbased.ExternalSort;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ExternalSortBlockingIO implements ExternalSort {
    private static final Logger LOGGER = LogManager.getLogger(ExternalSortBlockingIO.class);
    private Logger log = LOGGER;
    private int inputChunkSize;
    private int outputBufferSize;
    private String sortImpl;

    public ExternalSortBlockingIO(int inputChunkSize, int outputBufferSize, String sortImpl) {
        this.inputChunkSize = inputChunkSize;
        this.outputBufferSize = outputBufferSize;
        this.sortImpl = sortImpl;
    }

    @Override
    public void sort(final String intermediateFilePath, String inputFilePath, String outputPath, int K) throws Exception {
        DivideAndScatterJob dsJob = new DivideAndScatterJob(inputChunkSize, sortImpl, outputBufferSize);
        dsJob.divideAndScatter(intermediateFilePath, inputFilePath);

        GatherAndMergeJob gmJob = new GatherAndMergeJob(inputChunkSize, outputBufferSize);
        gmJob.gatherAndMerge(intermediateFilePath, outputPath, K);
    }
}

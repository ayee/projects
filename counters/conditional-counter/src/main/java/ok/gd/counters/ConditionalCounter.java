package ok.gd.counters;

import java.io.FileInputStream;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Created by olegklymchuk on 8/22/16.
 *
 */

public class ConditionalCounter implements AutoCloseable {

    protected String path;
    protected int numAttrs;
    protected FilterCondition filterCondition;
    protected int numThreads;
    protected ExecutorService executorService;

    public ConditionalCounter(String path, int numAttrs, FilterCondition filterCondition) throws ConditionalCounterException {

        init(path, numAttrs, filterCondition, 1);
    }

    public ConditionalCounter(String path, int numAttrs, FilterCondition filterCondition, int numThreads) throws ConditionalCounterException {

        init(path, numAttrs, filterCondition, numThreads);
    }

    protected void init(String path, int numAttrs, FilterCondition filterCondition, int numThreads) throws ConditionalCounterException {

        if(path == null || path.isEmpty()) {
            throw new ConditionalCounterException("Input path is null or empty");
        }

        if(numAttrs < 1) {
            throw new ConditionalCounterException("Invalid number of attributes has been specified");
        }

        this.path = path;
        this.numAttrs = numAttrs;
        this.filterCondition = filterCondition;

        this.numThreads = numThreads;
        executorService = Executors.newFixedThreadPool(numThreads);
    }

    public long count() throws ConditionalCounterException {

        long result = 0;

        try {
            FileInputStream fileInputStream = new FileInputStream(path);
            long fileSize = fileInputStream.getChannel().size();
            int recordSize = Record.size(numAttrs);
            long numRecords = fileSize / recordSize;

            long partitionSize = (numRecords / numThreads) * recordSize;

            if(partitionSize == 0) {
                //too small dataset, can be handled in current thread
                return new AsyncCounter(path, numAttrs, filterCondition, 0, numRecords * recordSize).call();
            }

            Map<Long, Long> ranges = new LinkedHashMap<>();
            long offset = 0;
            for(int i = 0; i < numThreads; ++i) {

                if((offset + partitionSize) > fileSize
                        || i == (numThreads - 1)) {
                    partitionSize = fileSize - offset;
                }

                ranges.put(offset, offset + partitionSize);
                offset += partitionSize;
            }

            List<AsyncCounter> asyncCounters = new LinkedList<>();

            for(Map.Entry<Long, Long> entry : ranges.entrySet()) {
                asyncCounters.add(new AsyncCounter(path, numAttrs, filterCondition, entry.getKey(), entry.getValue()));
            }

            List<Future<Long>> taskResults = executorService.invokeAll(asyncCounters);
            for(Future<Long> taskResult : taskResults) {
                result += taskResult.get();
            }

        } catch (Exception e) {
            throw new ConditionalCounterException("Failed to count records. Exception follows.", e);
        }


        return result;
    }

    public void close() throws Exception {

            executorService.shutdown();
    }

    private static class AsyncCounter implements Callable<Long> {

        protected String path;
        protected int numAttrs;
        protected FilterCondition filterCondition;
        protected long readFrom;
        protected long readTo;

        public AsyncCounter(String path, int numAttrs, FilterCondition filterCondition, long readFrom, long readTo) {

            this.path = path;
            this.numAttrs = numAttrs;
            this.filterCondition = filterCondition;

            this.readFrom = readFrom;
            this.readTo = readTo;
        }

        public Long call() throws Exception {

            long result = 0;

            try {
                FileInputStream fileInputStream = new FileInputStream(path);

                long skipped = 0;
                while(skipped < readFrom) {
                    skipped += fileInputStream.skip(readFrom - skipped);
                }

                int recordSize = Record.size(numAttrs);

                byte[] buffer = new byte[recordSize];

                int bytesRead = 0;
                while (bytesRead < readTo - readFrom) {
                    bytesRead += fileInputStream.read(buffer, 0, recordSize);
                    Record record = new Record(buffer);
                    if(filterCondition.check(record)) {
                        ++result;
                    }
                }

            } catch (Exception e) {
                throw new ConditionalCounterException("AsyncCounter has failed to count records. Exception follows.", e);
            }

            return result;
        }
    }

}

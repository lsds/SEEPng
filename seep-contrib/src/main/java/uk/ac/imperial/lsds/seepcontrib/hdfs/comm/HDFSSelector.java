package uk.ac.imperial.lsds.seepcontrib.hdfs.comm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.core.DataStoreSelector;
import uk.ac.imperial.lsds.seep.core.IBuffer;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HDFSSelector implements DataStoreSelector {

    final private static Logger LOG = LoggerFactory.getLogger(HDFSSelector.class);
    private Thread[] workers;
    private FileSystem fs;

    public HDFSSelector(String hdfsDefaultFS, Map<Integer, IBuffer> dataAdapters) {
        Path path = new Path(hdfsDefaultFS);
        try {
            Configuration conf = new Configuration() ;
            this.fs = FileSystem.get(path.toUri(), conf);

            RemoteIterator<LocatedFileStatus> statuses = fs.listFiles(path, true);

            int workerCount = dataAdapters.size();
            workers = new Thread[workerCount];

            Map<Integer, List<FileStatus>> workerStatuses = splitStatuses(statuses, workerCount);

            int threadNumber = 0;
            for (IBuffer ib : dataAdapters.values()) {
                Thread worker = new Thread(new HDFSReader(workerStatuses.get(threadNumber), ib));
                worker.setName("HDFSReader-" + threadNumber);
                workers[threadNumber] = worker;
                threadNumber++;
            }
        } catch (IOException ioe) {
            LOG.error("HDFS failed to fetch configuration");
            ioe.printStackTrace();
        }
    }

    /**
     * Divides the list of FileStatus between workerCount worker threads.
     * @param statuses The iterator of FileStatus to divide.
     * @param workerCount The number of workers to divide between.
     * @return A List of length workerCount containing a subset of the FileStatus'.
     */
    private Map<Integer, List<FileStatus>> splitStatuses(RemoteIterator<LocatedFileStatus> statuses, int workerCount) throws IOException {
        Map<Integer, List<FileStatus>> result = new HashMap<>();

        int i = 0;
        while (statuses.hasNext()) {
            int index = i % workerCount;
            LocatedFileStatus status = statuses.next();

            if (result.containsKey(index)) {
                result.get(index).add(status);
            } else {
                List<FileStatus> statusList = new ArrayList<>();
                statusList.add(status);
                result.put(index, statusList);
            }
        }

        return result;
    }

    @Override
    public DataStoreType type() {
        return DataStoreType.HDFS;
    }

    @Override
    public boolean initSelector() {
        return true;
    }

    @Override
    public boolean startSelector() {
        for (Thread worker : workers) {
            LOG.info("Starting HDFS worker: {}", worker.getName());
            worker.start();
        }
        return true;
    }

    @Override
    public boolean stopSelector() {
        return false;
    }

    private class HDFSReader implements Runnable {

        final private Logger LOG = LoggerFactory.getLogger(HDFSReader.class);
        private final List<FileStatus> statuses;
        private final IBuffer ib;
        private final String workerName = Thread.currentThread().getName();

        private HDFSReader(List<FileStatus> statuses, IBuffer ib) {
            this.statuses = statuses;
            this.ib = ib;
        }

        /**
         * This method iterates over the list of FileStatus assigned to this thread.
         * It reads their contents and pushes them to an IBuffer.
         */
        @Override
        public void run() {
            LOG.info("{} reading {} statuses", this.workerName, statuses.size());
            for (FileStatus f : statuses) {
                try {
                    FSDataInputStream inputStream = fs.open(f.getPath());
                    ReadableByteChannel channel = Channels.newChannel(inputStream);
                    int tuplesRead = ib.readFrom(channel);
                    LOG.info("{} read {} tuples from {}", this.workerName, tuplesRead, f.getPath().toString());
                } catch (IOException e) {
                    LOG.error("Error when attempting to open " + f.getPath().toString(), e);
                }
            }
        }
    }
}


package uk.ac.imperial.lsds.seepcontrib.hdfs.comm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.core.DataStoreSelector;
import uk.ac.imperial.lsds.seep.core.IBuffer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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

            FileStatus[] status = fs.listStatus(path);

            int workerCount = dataAdapters.size();
            workers = new Thread[workerCount];

            Map<Integer, List<FileStatus>> workerStatuses = splitStatuses(status, workerCount);

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
     * @param status The array of FileStatus to divide.
     * @param workerCount The number of workers to divide between.
     * @return A List of length workerCount containing a subset of the FileStatus'.
     */
    private Map<Integer, List<FileStatus>> splitStatuses(FileStatus[] status, int workerCount) {
        Map<Integer, List<FileStatus>> result = new HashMap<>();

        for (int i = 0; i < status.length; i++) {
            int index = i % workerCount;

            if (result.containsKey(index)) {
                result.get(index).add(status[i]);
            } else {
                List<FileStatus> statusList = new ArrayList<>();
                statusList.add(status[i]);
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

        private static final int BUFFER_SIZE = 4096;
        final private Logger LOG = LoggerFactory.getLogger(HDFSReader.class);
        private final List<FileStatus> statuses;
        private final IBuffer ib;

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
            for (FileStatus f : statuses) {
                try {
                    FSDataInputStream inputStream = fs.open(f.getPath());
                    byte buf[] = new byte[BUFFER_SIZE];
                    int bytesRead = inputStream.read(buf);
                    while (bytesRead >= 0) {
                        if (bytesRead == BUFFER_SIZE) {
                            ib.pushData(buf);
                        } else {
                            // On the final iteration the buf array won't be full
                            // and we shouldn't be pushing additional data.
                            ib.pushData(Arrays.copyOfRange(buf, 0, bytesRead));
                        }
                        bytesRead = inputStream.read(buf);
                    }
                } catch (IOException e) {
                    LOG.error("Error when attempting to open " + f.getPath(), e);
                }
            }
        }
    }
}


package uk.ac.imperial.lsds.seepcontrib;

import org.junit.Test;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.core.IBuffer;
import uk.ac.imperial.lsds.seep.tools.GenerateBinaryFile;
import uk.ac.imperial.lsds.seepcontrib.hdfs.comm.HDFSDataStream;
import uk.ac.imperial.lsds.seepcontrib.hdfs.comm.HDFSSelector;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.core.input.InputBuffer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

public class HDFSSelectorTest {

    @Test
    public void test() {
        Schema s = Schema.SchemaBuilder.getInstance().newField(Type.INT, "param1").newField(Type.INT, "param2").build();
        String path = "/tmp/test.data";
        int targetSize = 1; //1MB file

        // Take this generated file and upload it to your HDFS installation
        GenerateBinaryFile.createFile(s, path, targetSize);

        Properties p = new Properties();
        p.setProperty(WorkerConfig.MASTER_IP, "");
        p.setProperty(WorkerConfig.PROPERTIES_FILE, "");
        p.setProperty(WorkerConfig.SIMPLE_INPUT_QUEUE_LENGTH, "100");
        p.setProperty(WorkerConfig.LISTENING_IP, "");
        WorkerConfig wc = new WorkerConfig(p);

        IBuffer ib = InputBuffer.makeInputBufferFor(wc, null);

        Map<Integer, IBuffer> dataAdapters = new HashMap<>();
        dataAdapters.put(0, ib);

        String hdfsTestFilePath = "hdfs://..."; // Set this path to the uploaded, generated file.
        HDFSSelector hdfs = new HDFSSelector(hdfsTestFilePath, dataAdapters);
        HDFSDataStream hdfsStream = new HDFSDataStream(12, ib, s);

        hdfs.startSelector();

        int tuplesRead = 0;
        while(tuplesRead < 10){
            ITuple tuple = hdfsStream.pullDataItem(10);
            if(tuple == null) {
                continue;
            }
            tuplesRead++;
            int p1 = tuple.getInt("param1");
            int p2 = tuple.getInt("param2");
            System.out.println("P1: " + p1 + " P2: " + p2);
        }

        assertTrue(true);
    }
}

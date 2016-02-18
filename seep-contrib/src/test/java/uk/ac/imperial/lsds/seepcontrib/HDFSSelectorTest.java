package uk.ac.imperial.lsds.seepcontrib;

import org.junit.Test;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.core.IBuffer;
import uk.ac.imperial.lsds.seep.tools.GenerateBinaryFile;
import uk.ac.imperial.lsds.seep.util.Utils;
import uk.ac.imperial.lsds.seepcontrib.hdfs.comm.HDFSDataStream;
import uk.ac.imperial.lsds.seepcontrib.hdfs.comm.HDFSSelector;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.core.input.InputBuffer;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

public class HDFSSelectorTest {

    @Test
    public void test() {
        String path = "/tmp/test.data";
        String absPath = Utils.absolutePath(path);
        URI uri = null;
        try {
            uri = new URI(Utils.FILE_URI_SCHEME + absPath);
        } catch (URISyntaxException e) {
            e.printStackTrace();
            assertTrue(false);
        }

        Schema s = Schema.SchemaBuilder.getInstance().newField(Type.INT, "param1").newField(Type.INT, "param2").build();
        int targetSize = 1;

        GenerateBinaryFile.createFile(s, uri.toString(), targetSize);

        Properties p = new Properties();
        p.setProperty(WorkerConfig.MASTER_IP, "");
        p.setProperty(WorkerConfig.PROPERTIES_FILE, "");
        p.setProperty(WorkerConfig.SIMPLE_INPUT_QUEUE_LENGTH, "100");
        p.setProperty(WorkerConfig.LISTENING_IP, "");
        WorkerConfig wc = new WorkerConfig(p);

        IBuffer ib = InputBuffer.makeInputBufferFor(wc, null);

        Map<Integer, IBuffer> dataAdapters = new HashMap<>();
        dataAdapters.put(0, ib);

        HDFSSelector hdfs = new HDFSSelector(uri.toString(), dataAdapters);
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

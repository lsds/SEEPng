package uk.ac.imperial.lsds.seepcontrib.hdfs.comm;

import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.core.IBuffer;
import uk.ac.imperial.lsds.seep.core.InputAdapter;
import uk.ac.imperial.lsds.seep.core.InputAdapterReturnType;

import java.util.List;

public class HDFSDataStream implements InputAdapter {

    private final int streamId;
    private final ITuple iTuple;
    private IBuffer buffer;

    public HDFSDataStream(int streamId, IBuffer buffer, Schema expectedSchema) {
        this.streamId = streamId;
        this.buffer = buffer;
        this.iTuple = new ITuple(expectedSchema);
    }

    @Override
    public int getStreamId() {
        return this.streamId;
    }

    @Override
    public short returnType() {
        return InputAdapterReturnType.ONE.ofType();
    }

    @Override
    public DataStoreType getDataStoreType() {
        return DataStoreType.HDFS;
    }

    @Override
    public ITuple pullDataItem(int timeout) {
        byte[] data = this.buffer.read(timeout);
        if(data == null) {
            return null;
        }
        iTuple.setData(data);
        iTuple.setStreamId(streamId);
        return iTuple;
    }

    @Override
    public List<ITuple> pullDataItems(int timeout) {
        return null;
    }
}

package uk.ac.imperial.lsds.seepworker.core.input;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.comm.Comm;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.comm.protocol.MasterWorkerCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.ProtocolCommandFactory;
import uk.ac.imperial.lsds.seep.comm.protocol.WorkerWorkerCommand;
import uk.ac.imperial.lsds.seep.core.IBuffer;
import uk.ac.imperial.lsds.seep.core.InputAdapter;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;

public class CoreInput {
	
	final private static Logger LOG = LoggerFactory.getLogger(CoreInput.class);
	
	private WorkerConfig wc;
	private Map<Integer, Set<DataReference>> input;
	private Map<Integer, IBuffer> iBuffers;
	private List<InputAdapter> inputAdapters;
	
	public CoreInput(WorkerConfig wc, Map<Integer, Set<DataReference>> input, Map<Integer, IBuffer> iBuffers, List<InputAdapter> inputAdapters) {
		this.wc = wc;
		this.input = input;
		this.iBuffers = iBuffers;
		this.inputAdapters = inputAdapters;
		
		LOG.info("Configured CoreInput with {} inputAdapters", inputAdapters.size());
	}
	
	public List<InputAdapter> getInputAdapters(){
		return inputAdapters;
	}
	
	public boolean requiresConfigureSelectorOfType(DataStoreType type){
		for(InputAdapter ia : inputAdapters){
			if(ia.getDataStoreType().equals(type)){
				return true;
			}
		}
		return false;
	}
	
	public Map<Integer, IBuffer> getIBufferProvider(){
		return iBuffers;
	}

	public void requestInputConnections(Comm comm, Kryo k, InetAddress myIp) {
		for(Set<DataReference> i : input.values()) {
			for(DataReference dr : i) {
				if(dr.isManaged()) {
					// Create dataRef request and send to the worker
					WorkerWorkerCommand requestStreamDataReference = ProtocolCommandFactory.buildRequestDataReference(dr.getId(), myIp, wc.getInt(WorkerConfig.DATA_PORT));
					Connection targetConn = new Connection(dr.getEndPoint());
					comm.send_object_sync(requestStreamDataReference, targetConn, k);
				}
			}
		}
	}
	
}

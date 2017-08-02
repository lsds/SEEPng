package uk.ac.imperial.lsds.seepworker.core;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.DataReference.ServeMode;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.TransporterITuple;
import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.infrastructure.ControlEndPoint;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;
import uk.ac.imperial.lsds.seep.scheduler.Stage;
import uk.ac.imperial.lsds.seep.scheduler.StageType;
import uk.ac.imperial.lsds.seep.util.Utils;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;

public class ScheduleTask implements SeepTask {

	final private static Logger LOG = LoggerFactory.getLogger(ScheduleTask.class.getName());
	
	private int stageId;
	private int euId;
	private List<LogicalOperator> operators;
	private Iterator<LogicalOperator> opIt;
	private List<SeepTask> tasks;
	private Iterator<SeepTask> taskIterator;
	private API scApi = new SimpleCollector();
	
	// Optimizing for same schema
	private boolean sameSchema = false;
	private Schema schema = null;
	private ITuple data = null;
	private TransporterITuple d = null;
	
	private ScheduleTask(int euId, int stageId, List<LogicalOperator> operators) {
		this.stageId = stageId;
		this.euId = euId;
		this.operators = operators;
		this.tasks = new ArrayList<>();
		this.opIt = operators.iterator();
		while(opIt.hasNext()) {
			tasks.add(opIt.next().getSeepTask());
		}
		this.taskIterator = tasks.iterator();
		// TODO: initialize sameSchema here by actually checking if the schema is the same
		if(sameSchema && opIt.hasNext()) {
			schema = opIt.next().downstreamConnections().get(0).getSchema();
			data = new ITuple(schema);
			d = new TransporterITuple(schema);
			opIt = operators.iterator(); // reset
		}
	}
	
	public static ScheduleTask buildTaskFor(int id, Stage s, ScheduleDescription sd, WorkerConfig wc) {
		Deque<Integer> wrappedOps = s.getWrappedOperators();
		LOG.info("Building stage {}. Wraps {} operators", s.getStageId(), wrappedOps.size());
//		Deque<LogicalOperator> operators = new ArrayDeque<>();
		List<LogicalOperator> operators = new ArrayList<>();
		while(! wrappedOps.isEmpty()) {
			LogicalOperator lo = sd.getOperatorWithId(wrappedOps.poll());
			LOG.debug("op {} is part of stage {}", lo.getOperatorId(), s.getStageId());
			operators.add(lo);
		}
		
		Iterator <LogicalOperator> iter = operators.iterator();
		while (iter.hasNext()){		
			LogicalOperator nextOp = iter.next();
			if(nextOp.downstreamConnections().size() > 0) {
				Schema schema = nextOp.downstreamConnections().get(0).getSchema(); // 0 cause there's only 1
				System.out.println("MMM"+schema.fields().length);
				s.setOutputDataReferences(createOutputForTask(s,schema, id, wc));
			}
		}
		
		
		return new ScheduleTask(id, s.getStageId(), operators);
	}
	
	public int getStageId() {
		return stageId;
	}
	
	public int getEuId() {
		return euId;
	}
	
	@Override
	public void setUp() {
		if(! taskIterator.hasNext()) {
			taskIterator = tasks.iterator();
		}
		if(! opIt.hasNext()) {
			opIt = operators.iterator();
		}
	}

//	@Override
	@Deprecated
	public void _processData(ITuple data, API api) {
		API scApi = new SimpleCollector();
		SeepTask next = taskIterator.next(); // Get first, and possibly only task here
		// Check whether there are tasks ahead
		while(taskIterator.hasNext()) {
			// There is a next OP, we simply need to collect output
			next.processData(data, scApi);
			byte[] o = ((SimpleCollector)scApi).collectMem();
			LogicalOperator nextOp = opIt.next();
			if(! sameSchema) {
				Schema schema = nextOp.downstreamConnections().get(0).getSchema(); // 0 cause there's only 1
				data = new ITuple(schema);
			}
			data.setData(o);
			// Otherwise we simply forward the data
			next = taskIterator.next();
		}
		// Finally use real API for real forwarding
		next.processData(data, api);
		// Then reset iterators for more processing
		taskIterator = tasks.iterator();
		opIt = operators.iterator();
	}
	
	@Override
	public void processData(ITuple data, API api) {
		byte[] o = null;
		boolean taskProducedEmptResult = false;
		
		
		if (data == null) {
			return;
		}
		
		for(int i = 0; i < tasks.size() - 1; i++) {
			((SimpleCollector)scApi).reset();
			SeepTask next = tasks.get(i);
			next.processData(data, scApi);
			o = ((SimpleCollector)scApi).collectMem();
			
			if(o == null) {
				taskProducedEmptResult = true;
				d = null;
				data = null;
				break;
			}
			
			LogicalOperator nextOp = operators.get(i);
			Schema schema = nextOp.downstreamConnections().get(0).getSchema(); // 0 cause there's only 1
			System.out.println("AAA"+schema.fields().length);
			
			data = new ITuple(schema, o);
			
			d = new TransporterITuple(schema); // FIXME: can we get schema from OTuple
			d.setData(o);
		}
		
		if (!taskProducedEmptResult && data != null) {
			SeepTask next = tasks.get(tasks.size() -1);
			next.processData(data, api);
		}
		
//		SeepTask next = taskIterator.next(); // Get first, and possibly only task here
//		// Check whether there are tasks ahead
//		while(taskIterator.hasNext()) {
//			// There is a next OP, we simply need to collect output
//			next.processData(data, scApi);
////			byte[] o = ((SimpleCollector)scApi).collect();
//			OTuple o = ((SimpleCollector)scApi).collect();
//			LogicalOperator nextOp = opIt.next();
//			if(! sameSchema) {
//				Schema schema = nextOp.downstreamConnections().get(0).getSchema(); // 0 cause there's only 1
//				data = new ITuple(schema);
//				d = new TransporterITuple(schema); // FIXME: can we get schema from OTuple
//			}
//			if(d == null) {
//				Schema schema = nextOp.downstreamConnections().get(0).getSchema();
//				d = new TransporterITuple(schema);
//			}
//			Object[] values = o.getValues();
//			d.setValues(values);
//			// Otherwise we simply forward the data
//			next = taskIterator.next();
//		}
//		
//		// Finally use real API for real forwarding
//		next.processData(data, api);
//		// Then reset iterators for more processing
//		taskIterator = tasks.iterator();
//		opIt = operators.iterator();
	}
	
	
	public void __processData(ITuple data, API api) {
		SeepTask next = taskIterator.next(); // Get first, and possibly only task here
		// Check whether there are tasks ahead
		while(taskIterator.hasNext()) {
			// There is a next OP, we simply need to collect output
			next.processData(data, scApi);
//			byte[] o = ((SimpleCollector)scApi).collect();
			OTuple o = ((SimpleCollector)scApi).collect();
			LogicalOperator nextOp = opIt.next();
			if(! sameSchema) {
				Schema schema = nextOp.downstreamConnections().get(0).getSchema(); // 0 cause there's only 1
				data = new ITuple(schema);
				d = new TransporterITuple(schema); // FIXME: can we get schema from OTuple
			}
			if(d == null) {
				Schema schema = nextOp.downstreamConnections().get(0).getSchema();
				d = new TransporterITuple(schema);
			}
			Object[] values = o.getValues();
			d.setValues(values);
			// Otherwise we simply forward the data
			next = taskIterator.next();
		}
		
		// Finally use real API for real forwarding
		next.processData(data, api);
		// Then reset iterators for more processing
		taskIterator = tasks.iterator();
		opIt = operators.iterator();
	}
	
	public boolean hasMoreTasks() {
		return taskIterator.hasNext();
	}

	@Override
	public void processDataGroup(List<ITuple> dataBatch, API api) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		if(taskIterator.hasNext()){
			taskIterator.next().close();
		}
		else{
			taskIterator = tasks.iterator();
		}
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("Stage: " + this.stageId + ", running on: " + this.euId);
		sb.append(Utils.NL);
		StringBuffer tasksDescr = new StringBuffer();
		for(LogicalOperator lo : operators) {
			tasksDescr.append(" t: " + lo.getOperatorId() + "-> " + lo.getSeepTask().toString());
			tasksDescr.append(Utils.NL);
		}
		sb.append(tasksDescr.toString());
		return sb.toString();
	}
	
	
	
	
	
	private static Map<Integer, Set<DataReference>> createOutputForTask(Stage s, Schema schema, int id, WorkerConfig wc) {
		// Master did not assign output, so we need to create it here
		// Althouth output is indexed on an integer, this is for compatibility with
		// downstream interfaces. It will always be the stageId
		Map<Integer, Set<DataReference>> output = new HashMap<>();
		
		if(s.hasDependantWithPartitionedStage()) {
			// create a DR per partition, that are managed
			// TODO: how to get the number of partitions
			int numPartitions = wc.getInt(WorkerConfig.SHUFFLE_NUM_PARTITIONS);
			int outputId = s.getStageId();
			Set<DataReference> drefs = new HashSet<>();
			// TODO: create a DR per partition and assign the partitionSeqId
			for(int i = 0; i < numPartitions; i++) {
				DataStore dataStore = new DataStore(schema, DataStoreType.IN_MEMORY);
				ControlEndPoint cep = new ControlEndPoint(id, wc.getString(WorkerConfig.WORKER_IP), wc.getInt(WorkerConfig.CONTROL_PORT));
				DataReference dr = null;
				int partitionId = i;
				dr = DataReference.makeManagedAndPartitionedDataReference(dataStore, cep, ServeMode.STORE, partitionId);
				drefs.add(dr);
			}
			output.put(outputId, drefs);
		}
		else {
			// create a single DR, that is managed
			int outputId = s.getStageId();
			Set<DataReference> drefs = new HashSet<>();
			DataStore dataStore = new DataStore(schema, DataStoreType.IN_MEMORY);
			ControlEndPoint cep = new ControlEndPoint(id, wc.getString(WorkerConfig.WORKER_IP), wc.getInt(WorkerConfig.CONTROL_PORT));
			DataReference dr = null;
			// TODO: is this enough?
			if(s.getStageType().equals(StageType.SINK_STAGE)) {
				dr = DataReference.makeSinkExternalDataReference(dataStore);
			}
			else {
				dr = DataReference.makeManagedDataReference(dataStore, cep, ServeMode.STORE);
			}
			drefs.add(dr);
			output.put(outputId, drefs);
		}
		return output;
	}
}

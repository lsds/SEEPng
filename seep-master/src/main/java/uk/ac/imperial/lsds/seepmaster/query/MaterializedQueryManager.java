package uk.ac.imperial.lsds.seepmaster.query;

import java.util.*;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.DataReference.ServeMode;
import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.operator.DownstreamConnection;
import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.operator.Operator;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.api.operator.UpstreamConnection;
import uk.ac.imperial.lsds.seep.api.operator.sinks.MarkerSink;
import uk.ac.imperial.lsds.seep.comm.Comm;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.comm.protocol.ProtocolCommandFactory;
import uk.ac.imperial.lsds.seep.comm.protocol.SeepCommand;
import uk.ac.imperial.lsds.seep.comm.serialization.KryoFactory;
import uk.ac.imperial.lsds.seep.infrastructure.ControlEndPoint;
import uk.ac.imperial.lsds.seep.infrastructure.api.RestAPINodeDescription;
import uk.ac.imperial.lsds.seep.util.Utils;
import uk.ac.imperial.lsds.seepmaster.LifecycleManager;
import uk.ac.imperial.lsds.seepmaster.MasterConfig;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.ExecutionUnit;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.InfrastructureManager;

import com.esotericsoftware.kryo.Kryo;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.api.RestAPINodeManager;


public class MaterializedQueryManager implements QueryManager {

	final private Logger LOG = LoggerFactory.getLogger(MaterializedQueryManager.class);
	
	private MasterConfig mc;
	private static MaterializedQueryManager qm;
	private LifecycleManager lifeManager;
	private SeepLogicalQuery slq;
	private int executionUnitsRequiredToStart;
	private InfrastructureManager inf;
	private Map<Integer, ControlEndPoint> opToEndpointMapping;
	private Map<Integer, RestAPINodeManager> opToNodeInformationMapping;
	private final Comm comm;
	private final Kryo k;
	
	// Query information
	private String pathToQueryJar;
	private String definitionClassName;
	private String[] queryArgs;
	private String composeMethodName;
	private short queryType;

	private boolean enableRestAPI;

	// convenience method for testing
	public static MaterializedQueryManager buildTestMaterializedQueryManager(SeepLogicalQuery lsq, 
			InfrastructureManager inf, Map<Integer, ControlEndPoint> mapOpToEndPoint, Comm comm) {
		return new MaterializedQueryManager(lsq, inf, mapOpToEndPoint, comm);
	}
	
	private MaterializedQueryManager(SeepLogicalQuery lsq, InfrastructureManager inf, 
			Map<Integer, ControlEndPoint> opToEndpointMapping, Comm comm) {
		this.slq = lsq;
		this.executionUnitsRequiredToStart = this.computeRequiredExecutionUnits(lsq);
		this.inf = inf;
		this.opToEndpointMapping = opToEndpointMapping;
		this.opToNodeInformationMapping = null;
		this.comm = comm;
		this.k = KryoFactory.buildKryoForProtocolCommands(this.getClass().getClassLoader());
		this.enableRestAPI = false;
	}
	
	private MaterializedQueryManager(InfrastructureManager inf, Map<Integer, ControlEndPoint> mapOpToEndPoint, 
			Comm comm, LifecycleManager lifeManager, MasterConfig mc) {
		this.inf = inf;
		this.opToEndpointMapping = mapOpToEndPoint;
		this.opToNodeInformationMapping = null;
		this.comm = comm;
		this.lifeManager = lifeManager;
		this.k = KryoFactory.buildKryoForProtocolCommands(this.getClass().getClassLoader());
		this.mc = mc;
		this.enableRestAPI = false;
	}
	
	public static MaterializedQueryManager getInstance(InfrastructureManager inf, Map<Integer, ControlEndPoint> mapOpToEndPoint, 
			Comm comm, LifecycleManager lifeManager, MasterConfig mc) {
		if(qm == null){
			return new MaterializedQueryManager(inf, mapOpToEndPoint, comm, lifeManager, mc);
		}
		else{
			return qm;
		}
	}
	
	private boolean canStartExecution() {
		return inf.executionUnitsAvailable() >= executionUnitsRequiredToStart;
	}
	
	@Override
	public boolean loadQueryFromParameter(short queryType, SeepLogicalQuery slq, String pathToQueryJar, String definitionClass, 
			String[] queryArgs, String composeMethod) {
		boolean allowed = lifeManager.canTransitTo(LifecycleManager.AppStatus.QUERY_SUBMITTED);
		if(!allowed){
			LOG.error("Attempt to violate application lifecycle");
			return false;
		}
		this.slq = slq;
		this.queryType = queryType;
		this.pathToQueryJar = pathToQueryJar;
		this.definitionClassName = definitionClass;
		this.queryArgs = queryArgs;
		this.composeMethodName = composeMethod;
		LOG.debug("Logical query loaded: {}", slq.toString());
		this.executionUnitsRequiredToStart = this.computeRequiredExecutionUnits(slq);
		LOG.info("New query requires: {} units to start execution", this.executionUnitsRequiredToStart);
		lifeManager.tryTransitTo(LifecycleManager.AppStatus.QUERY_SUBMITTED);
		return true;
	}
	
	@Override
	public boolean loadQueryFromFile(short queryType, String pathToQueryJar, String definitionClass, String[] queryArgs, String composeMethod, boolean enable_rest_api) {
		boolean allowed = lifeManager.canTransitTo(LifecycleManager.AppStatus.QUERY_SUBMITTED);
		if(!allowed){
			LOG.error("Attempt to violate application lifecycle");
			return false;
		}
		this.pathToQueryJar = pathToQueryJar;
		// get logical query 
		this.slq = Utils.executeComposeFromQuery(pathToQueryJar, definitionClass, queryArgs, "compose");
		LOG.debug("Logical query loaded: {}", slq.toString());
		this.executionUnitsRequiredToStart = this.computeRequiredExecutionUnits(slq);
		LOG.info("New query requires: {} units to start execution", this.executionUnitsRequiredToStart);
		lifeManager.tryTransitTo(LifecycleManager.AppStatus.QUERY_SUBMITTED);
		return true;
	}
	
	@Override
	public boolean deployQueryToNodes() {
		boolean allowed = lifeManager.canTransitTo(LifecycleManager.AppStatus.QUERY_DEPLOYED);
		if(!allowed){
			LOG.error("Attempt to violate application lifecycle");
			return false;
		}
		// Check whether there are sufficient execution units to deploy query
		if(!canStartExecution()){
			LOG.warn("Cannot deploy query, not enough nodes. Required: {}, available: {}"
					, executionUnitsRequiredToStart, inf.executionUnitsAvailable());
			return false;
		}
		// Build mapping for logicalquery
		if(this.opToEndpointMapping != null){
			LOG.info("Using provided mapping for logicalQuery...");
			// TODO: do this
		} 
		else {
			LOG.info("Building mapping for logicalQuery...");
			this.opToEndpointMapping = createMappingOfOperatorWithEndPoint(slq);
			if (enableRestAPI) {
				this.opToNodeInformationMapping = createNodeDescriptionServers(opToEndpointMapping);
			}
		}
		// Materialize all DataReference once there exists a mapping
		Map<Integer, Map<Integer, Set<DataReference>>> outputs = generateOutputDataReferences(slq, opToEndpointMapping);
		Map<Integer, Map<Integer, Set<DataReference>>> inputs = generateInputDataReferences(slq, outputs);
		
		LOG.debug("Mapping for logicalQuery...OK {}", Utils.printMap(opToEndpointMapping));
		Set<Integer> involvedEUId = getInvolvedEuIdIn(opToEndpointMapping.values());
		Set<Connection> connections = inf.getConnectionsTo(involvedEUId);
		sendQueryToNodes(connections, definitionClassName, queryArgs, composeMethodName);
		sendMaterializeTaskToNodes(connections, this.opToEndpointMapping, inputs, outputs);
		lifeManager.tryTransitTo(LifecycleManager.AppStatus.QUERY_DEPLOYED);
		return true; 
	}
	
	private Set<Integer> getInvolvedEuIdIn(Collection<ControlEndPoint> values) {
		Set<Integer> involvedEUs = new HashSet<>();
		for(ControlEndPoint ep : values) {
			involvedEUs.add(ep.getId());
		}
		return involvedEUs;
	}
	
	private Map<Integer, Map<Integer, Set<DataReference>>> generateInputDataReferences(SeepLogicalQuery slq, Map<Integer, Map<Integer, Set<DataReference>>> outputs) {
		Map<Integer, Map<Integer, Set<DataReference>>> inputs = new HashMap<>();
		for(LogicalOperator lo : slq.getAllOperators()) {
			int opId = lo.getOperatorId();
			Map<Integer, Set<DataReference>> input = new HashMap<>();
			for(UpstreamConnection uc : lo.upstreamConnections()) {
				int streamId = uc.getStreamId();
				// Find all DataReferences that produce to this streamId filter by upstream operator
				Operator upstreamOp = uc.getUpstreamOperator();
				if(upstreamOp != null) {
					int upstreamOpId = upstreamOp.getOperatorId();
					for(Entry<Integer, Set<DataReference>> produces : outputs.get(upstreamOpId).entrySet()) {
						if(produces.getKey() == streamId) {
							if(! input.containsKey(streamId)) {
								input.put(streamId, new HashSet<>());
							}
							input.get(streamId).addAll(produces.getValue());
						}
					}
				}
				else {
					// This can occur when sources simply mark data origin. In this case we can create the 
					// DataReference directly
					DataReference dRef = DataReference.makeExternalDataReference(uc.getDataStore());
					// Then we add the DataReferences
					if(! input.containsKey(streamId)) {
						input.put(streamId, new HashSet<>());
					}
					input.get(streamId).add(dRef);
				}
				
			}
			inputs.put(opId, input);
		}
		return inputs;
	}
	
	private Map<Integer, Map<Integer, Set<DataReference>>> generateOutputDataReferences(SeepLogicalQuery slq, Map<Integer, ControlEndPoint> mapping) {
		Map<Integer, Map<Integer, Set<DataReference>>> outputs = new HashMap<>();
		// Generate per operator the dataReferences it produces
		for(LogicalOperator lo : slq.getAllOperators()) {
			Map<Integer, Set<DataReference>> output = new HashMap<>();
			int opId = lo.getOperatorId();
			ControlEndPoint ep = mapping.get(opId);
			// One dataReference per downstream, group by streamId
			for(DownstreamConnection dc : lo.downstreamConnections()) {
				DataStore dataStore = dc.getExpectedDataStoreOfDownstream();
				DataReference dref = null;
				if(dc.getDownstreamOperator() instanceof MarkerSink) {
					dref = DataReference.makeSinkExternalDataReference(dataStore);
				}
				else {
					dref = DataReference.makeManagedDataReferenceWithOwner(opId, dataStore, ep, ServeMode.STREAM);
				}
				int streamId = dc.getStreamId();
				if(! output.containsKey(streamId)) {
					output.put(streamId, new HashSet<>());
				}
				output.get(streamId).add(dref);
			}
			outputs.put(opId, output);
		}
		return outputs;
	}

	@Override
	public boolean startQuery() {
		boolean allowed = lifeManager.canTransitTo(LifecycleManager.AppStatus.QUERY_RUNNING);
		if(!allowed){
			LOG.error("Attempt to violate application lifecycle");
			return false;
		}
		// TODO: take a look at the following two lines. Stateless is good to keep everything lean. Yet consider caching
		Set<Integer> involvedEUId = getInvolvedEuIdIn(opToEndpointMapping.values());
		Set<Connection> connections = inf.getConnectionsTo(involvedEUId);
		// Send start query command
		SeepCommand start = ProtocolCommandFactory.buildStartQueryCommand();
		comm.send_object_sync(start, connections, k);
		lifeManager.tryTransitTo(LifecycleManager.AppStatus.QUERY_RUNNING);
		return true;
	}
	
	@Override
	public boolean stopQuery() {
		boolean allowed = lifeManager.canTransitTo(LifecycleManager.AppStatus.QUERY_STOPPED);
		if(!allowed){
			LOG.error("Attempt to violate application lifecycle");
			return false;
		}
		// TODO: take a look at the following two lines. Stateless is good to keep everything lean. Yet consider caching
		Set<Integer> involvedEUId = getInvolvedEuIdIn(opToEndpointMapping.values());
		Set<Connection> connections = inf.getConnectionsTo(involvedEUId);
		
		// Send start query command
		SeepCommand stop = ProtocolCommandFactory.buildStopQueryCommand();
		comm.send_object_sync(stop, connections, k);
		lifeManager.tryTransitTo(LifecycleManager.AppStatus.QUERY_STOPPED);
		return true;
	}

	@Override
	public Map<String, Object> extractQueryOperatorsInformation() {
		LOG.info("Extracting query operators in Materialized Query Manager");

		Map<String, Object> qpInformation = new HashMap<String, Object>();

		List<Object> nodes = new ArrayList<Object>();
		List<Object> edges = new ArrayList<Object>();

        for (LogicalOperator lo : slq.getSources()) {
            nodes.add(process_node_lo_information(lo, "graph_type_source"));
            edges.addAll(process_edges_lo_information(lo));
        }

        for (LogicalOperator lo : slq.getAllOperators()) {
            nodes.add(process_node_lo_information(lo, "graph_type_query"));
            edges.addAll(process_edges_lo_information(lo));
        }

        LogicalOperator sink = slq.getSink();
        nodes.add(process_node_lo_information(sink, "graph_type_sink"));
        edges.addAll(process_edges_lo_information(sink));

		qpInformation.put("nodes", nodes);
		qpInformation.put("edges", edges);

		return qpInformation;
	}

	private Map<String, Object> process_node_lo_information(LogicalOperator lo, String type) {
		Map<String, Object> nDetails = new HashMap<>();

		nDetails.put("id", "" + lo.getOperatorId());
		nDetails.put("type", "graph_type_query");

		//if (c.getOpContext().getOperatorStaticInformation() != null) {
		//    nDetails.put("ip", c.getOpContext().getOperatorStaticInformation().getMyNode().getIp());
		//    nDetails.put("port", c.getOpContext().getOperatorStaticInformation().getMyNode().getPort());
		//}

		Map<String, Object> nData = new HashMap<String, Object>();
		nData.put("data", nDetails);

		return nData;
	}

	private List<Object> process_edges_lo_information(LogicalOperator lo) {
		List<Object> edges = new ArrayList<Object>();

		Iterator<DownstreamConnection> iter = lo.downstreamConnections().iterator();
		while (iter.hasNext()) {
			DownstreamConnection dc = iter.next();
			Map<String, Object> eDetails = new HashMap<>();
			eDetails.put("streamid", lo.getOperatorId() + "-" + dc.getStreamId());
			eDetails.put("source", "" + lo.getOperatorId());
			eDetails.put("target", "" + dc.getDownstreamOperator().getOperatorId());
			eDetails.put("type", "graph_edge_defaults");
			Map<String, Object> eData = new HashMap<String, Object>();
			eData.put("data", eDetails);
			edges.add(eData);
		}

		return edges;
	}

	public Map<Integer, ControlEndPoint> createMappingOfOperatorWithEndPoint(SeepLogicalQuery slq) {
		Map<Integer, ControlEndPoint> mapping = new HashMap<>();
		for(LogicalOperator lso : slq.getAllOperators()){
			int opId = lso.getOperatorId();
			ExecutionUnit eu = inf.getExecutionUnit();
			ControlEndPoint ep = eu.getControlEndPoint();

			LOG.debug("LogicalOperator: {} will run on: {} -> ({})", opId, ep.getId(), ep.getIp().toString());
			mapping.put(opId, ep);
		}
		return mapping;
	}



	public Map<Integer, RestAPINodeManager> createNodeDescriptionServers(Map<Integer, ControlEndPoint> operatorsWithEndPointMap) {
		Map<Integer, RestAPINodeManager> mapping = new HashMap<>();

		for (Map.Entry<Integer, ControlEndPoint> entry : operatorsWithEndPointMap.entrySet()) {
			Integer opId = entry.getKey();
			ControlEndPoint endPointInfo = entry.getValue();

			RestAPINodeManager currentNodeDescription = new RestAPINodeManager();
			currentNodeDescription.addToRegistry("/nodedescription",  new RestAPINodeDescription(endPointInfo));
			currentNodeDescription.startServer(endPointInfo.getPort() + 1000);

			mapping.put(opId, currentNodeDescription);
		}

		return mapping;
	}

	public void setEnableRestAPI (boolean enableRestAPI_opt) {
		this.enableRestAPI = enableRestAPI_opt;
	}

	private int computeRequiredExecutionUnits(SeepLogicalQuery lsq) {
		return lsq.getAllOperators().size();
	}
	
	private void sendQueryToNodes(Set<Connection> connections, String definitionClassName, String[] queryArgs, String composeMethodName) {
		// Send data file to nodes
		byte[] queryFile = Utils.readDataFromFile(pathToQueryJar);
		LOG.info("Sending query file of size: {} bytes", queryFile.length);
		SeepCommand code = ProtocolCommandFactory.buildCodeCommand(queryType, queryFile, definitionClassName, queryArgs, composeMethodName);
		comm.send_object_sync(code, connections, k);
		LOG.info("Sending query file...DONE!");
	}
	
	private void sendMaterializeTaskToNodes(
			Set<Connection> connections, 
			Map<Integer, ControlEndPoint> mapping, 
			Map<Integer, Map<Integer, Set<DataReference>>> inputs, 
			Map<Integer, Map<Integer, Set<DataReference>>> outputs) {
		LOG.info("Sending materialize task command to nodes...");
		SeepCommand materializeCommand = ProtocolCommandFactory.buildMaterializeTaskCommand(mapping, inputs, outputs);
		comm.send_object_sync(materializeCommand, connections, k);
		LOG.info("Sending materialize task command to nodes...OK");
	}
	
}

package uk.ac.imperial.lsds.seepmaster.query;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.QueryExecutionMode;
import uk.ac.imperial.lsds.seep.api.QueryType;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.comm.Comm;
import uk.ac.imperial.lsds.seep.errors.NotImplementedException;
import uk.ac.imperial.lsds.seep.infrastructure.ControlEndPoint;
import uk.ac.imperial.lsds.seep.infrastructure.SeepEndPoint;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;
import uk.ac.imperial.lsds.seep.util.Utils;
import uk.ac.imperial.lsds.seepmaster.LifecycleManager;
import uk.ac.imperial.lsds.seepmaster.MasterConfig;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.InfrastructureManager;

public class GenericQueryManager implements QueryManager {

	final private Logger LOG = LoggerFactory.getLogger(GenericQueryManager.class);
	
	private MasterConfig mc;
	// Singleton instance made available to client (Master)
	private static GenericQueryManager gqm;
	// The actual QueryManager backend
	private QueryManager qm;
	
	private InfrastructureManager inf;
	private Map<Integer, ControlEndPoint> opToEndpointMapping;
	private Comm comm;
	private LifecycleManager lifeManager;

	private boolean enableRestAPI;
	
	public static GenericQueryManager getInstance(InfrastructureManager inf, 
			Map<Integer, ControlEndPoint> mapOpToEndPoint, 
			Comm comm, 
			LifecycleManager lifeManager, 
			MasterConfig mc){
		if(gqm == null){
			gqm = new GenericQueryManager(inf, mapOpToEndPoint, comm, lifeManager, mc);
		}
		return gqm;
	}
	
	private GenericQueryManager(InfrastructureManager inf, 
			Map<Integer, ControlEndPoint> mapOpToEndPoint, 
			Comm comm, 
			LifecycleManager lifeManager, 
			MasterConfig mc){
		this.inf = inf;
		this.opToEndpointMapping = mapOpToEndPoint;
		this.comm = comm;
		this.lifeManager = lifeManager;
		this.mc = mc;
		this.enableRestAPI = false;
	}
	
	@Override
	public boolean loadQueryFromParameter(short queryType, SeepLogicalQuery slq, String pathToQueryJar, 
			String definitionClassName, String[] queryArgs, String composeMethodName) {
		// Check whether the action is valid, but GenericQueryManager does not change Lifecycle
		boolean allowed = lifeManager.canTransitTo(LifecycleManager.AppStatus.QUERY_SUBMITTED);
		if(!allowed){
			LOG.error("Attempt to violate application lifecycle");
			return false;
		}
		LOG.debug("Logical query loaded: {}", slq.toString());
		// This is the only operation fully handled by QueryManager
		// After checking queryExecutionMode it creates an appropriate queryManager and
		// delegates all operations to that one
		QueryExecutionMode qem = slq.getQueryExecutionMode();
		qm = this.getQueryManagerForExecutionMode(qem, queryType);
		boolean success = qm.loadQueryFromParameter(queryType, slq, pathToQueryJar, definitionClassName, queryArgs, composeMethodName);
		return success;
	}
	
	@Override
	public boolean loadQueryFromFile(short queryType, String pathToQueryJar, String definitionClassName, String[] queryArgs, String composeMethodName, boolean enable_rest_api) {
		// Check whether the action is valid, but GenericQueryManager does not change Lifecycle
		boolean allowed = lifeManager.canTransitTo(LifecycleManager.AppStatus.QUERY_SUBMITTED);
		if(!allowed){
			LOG.error("Attempt to violate application lifecycle");
			return false;
		}

		this.setEnableRestAPI(enable_rest_api);
		// TODO: do something with the type at this point
		if(queryType == QueryType.SEEPLOGICALQUERY.ofType()) {
			// get logical query
			SeepLogicalQuery slq = Utils.executeComposeFromQuery(pathToQueryJar, definitionClassName, queryArgs, composeMethodName);
			if (slq == null) {
				LOG.error("Failed to load class " + definitionClassName + " from file " + pathToQueryJar);
				return false;
			}
			return this.loadQueryFromParameter(queryType, slq, pathToQueryJar, definitionClassName, queryArgs, composeMethodName);
		}
		else if (queryType == QueryType.SCHEDULE.ofType()) {
			ScheduleDescription schedule = Utils.executeComposeFromQuery(pathToQueryJar, definitionClassName, queryArgs, composeMethodName);
			if (schedule == null) {
				LOG.error("Failed to load class " + definitionClassName + " from file " + pathToQueryJar);
				return false;
			}
			// Otherwise create the scheduleQueryManager
			QueryExecutionMode qem = QueryExecutionMode.ALL_SCHEDULED;
			qm = this.getQueryManagerForExecutionMode(qem, queryType);
			// and load the scheduleDescription directly
			((ScheduledQueryManager)qm).loadSchedule(schedule, pathToQueryJar, definitionClassName, queryArgs, composeMethodName);
			return true;
		}
		LOG.error("Unrecognized Query type or non implemented");
		return false;
	}
	
	private QueryManager getQueryManagerForExecutionMode(QueryExecutionMode qem, short queryType) {
		QueryManager qm = null;
		switch (qem){
		case ALL_MATERIALIZED:
			LOG.info("Creating MaterializedQueryManager...");
			qm = MaterializedQueryManager.getInstance(inf, opToEndpointMapping, comm, lifeManager, mc);
			LOG.info("Creating MaterializedQueryManager...OK");
			break;
		case ALL_SCHEDULED:
			LOG.info("Creating ScheduledQueryManager...");
			qm = ScheduledQueryManager.getInstance(inf, comm, lifeManager, mc, queryType);
			LOG.info("Creating ScheduledQueryManager...OK");
			break;
		case AUTOMATIC_HYBRID:
			throw new NotImplementedException("Not implemented (?)");
		default:
			LOG.error("Execution Mode not supported !");
			throw new NotImplementedException("Most likely lacks implementation (?)");
		}
		return qm;
	}

	@Override
	public boolean deployQueryToNodes() {
		return this.qm.deployQueryToNodes();
	}

	@Override
	public boolean startQuery() {
		return this.qm.startQuery();
	}

	@Override
	public boolean stopQuery() {
		return this.qm.stopQuery();
	}

	@Override
	public void setEnableRestAPI(boolean enableRestAPI_opt) {
		this.enableRestAPI = enableRestAPI_opt;
	}

	@Override
	public Map<String, Object> extractQueryOperatorsInformation() {
		Map<String, Object> nDetails = new HashMap<>();

		// Check whether the action is valid, but GenericQueryManager does not change Lifecycle
		boolean allowed = lifeManager.canTransitTo(LifecycleManager.AppStatus.QUERY_DEPLOYED) || lifeManager.canTransitTo(LifecycleManager.AppStatus.QUERY_SUBMITTED);
		if(!allowed){
			LOG.error("Application trying to extract query operators information without being submitted");
			return nDetails;
		}

		return qm.extractQueryOperatorsInformation();
	}

	public QueryManager getQueryManager() {
		return qm;
	}

}

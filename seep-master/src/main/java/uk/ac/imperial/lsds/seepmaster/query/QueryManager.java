package uk.ac.imperial.lsds.seepmaster.query;

import uk.ac.imperial.lsds.seep.api.QueryExecutionMode;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;

import java.util.Map;

public interface QueryManager {

	public boolean loadQueryFromParameter(short queryType, SeepLogicalQuery slq, String pathToQueryJar,  String definitionClass, String[] queryArgs, String composeMethod);
	public boolean loadQueryFromFile(short queryType, String pathToQueryJar, String definitionClass, String[] queryArgs, String composeMethod, boolean enable_rest_api);
	
	public boolean deployQueryToNodes();
	public boolean startQuery();
	public boolean stopQuery();

	public void setEnableRestAPI(boolean enableRestAPI_opt);
	public Map<String, Object> extractQueryOperatorsInformation();
	
}

package uk.ac.imperial.lsds.seepcontrib.esper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.eclipse.jetty.util.MultiMap;

import com.espertech.esper.collection.Pair;

import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.infrastructure.api.RestAPIRegistryEntry;

public class RestAPIEsperGetMatches implements RestAPIRegistryEntry {

	private EsperSingleQueryOperator operator;
	
	public RestAPIEsperGetMatches(EsperSingleQueryOperator operator) {
		this.operator = operator;
	}
	
	@Override
	public Object getAnswer(MultiMap<String> reqParameters) {
		List<Pair<Long,OTuple>> result = new ArrayList<>();
		List<Pair<Long,OTuple>> cache = this.operator.getMatchCache();

		/*
		 * Should we keep all matches?
		 */
		boolean keep = false;
		if (reqParameters.containsKey("keep"))
			keep = Boolean.valueOf(reqParameters.getValue("keep",0));

		/*
		 * Do we have a time window?
		 */
		if (reqParameters.containsKey("start")
				&& reqParameters.containsKey("stop")) {
			
			long start = Long.valueOf(reqParameters.getValue("start",0));
			long stop  = Long.valueOf(reqParameters.getValue("stop",0));
			
			List<Pair<Long,OTuple>> tmpResult = new ArrayList<>();
			synchronized(cache) {
				Iterator<Pair<Long,OTuple>> iter = cache.iterator();
				while (iter.hasNext()) {
					Pair<Long,OTuple> pair_data = iter.next();
					Long t_payload_timestamp = pair_data.getFirst();
					OTuple t = pair_data.getSecond();
					if (start <= t_payload_timestamp && t_payload_timestamp <= stop) {
						tmpResult.add(new Pair<>(t_payload_timestamp, t));
						if (!keep) 
							iter.remove();
					}
				}
			}
			
			
			/*
			 * Is there a step to consider?
			 */
			long step = -1;
			if (reqParameters.containsKey("step"))
				step = Long.valueOf(reqParameters.getValue("step",0));
			
			if (step != -1) {
				
				long stepEnd = start + step;
				int i = 0;
				do {
					if (i < tmpResult.size()) {
						Pair<Long,OTuple> pair_data = tmpResult.get(i);
						Long t_payload_timestamp = pair_data.getFirst();
						OTuple t = pair_data.getSecond();
						if (t_payload_timestamp < stepEnd) {
							result.add(new Pair<Long, OTuple>(t_payload_timestamp, t));
							while ((t_payload_timestamp < stepEnd) && i < tmpResult.size()) {
								pair_data = tmpResult.get(i++);
								t_payload_timestamp = pair_data.getFirst();
								t = pair_data.getSecond();
							}
						}
						else {
							result.add(null);
						}
					}
					else 
						result.add(null);
					
					stepEnd += step;
				}
				while (stepEnd <= stop);
			}
			else {
				result = tmpResult; 
			}
		}
		else {
			/*
			 * No time window
			 */
			synchronized(cache) {
				result.addAll(cache);
				if (!keep) 
					cache.clear();
			}		
		}
		
		return result;
	}

}

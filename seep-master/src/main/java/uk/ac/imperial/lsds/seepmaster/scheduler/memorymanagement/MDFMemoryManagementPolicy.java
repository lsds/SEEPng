package uk.ac.imperial.lsds.seepmaster.scheduler.memorymanagement;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.core.DatasetMetadata;
import uk.ac.imperial.lsds.seep.core.DatasetMetadataPackage;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;
import uk.ac.imperial.lsds.seep.scheduler.Stage;

public class MDFMemoryManagementPolicy implements MemoryManagementPolicy {

	final private Logger LOG = LoggerFactory.getLogger(MDFMemoryManagementPolicy.class);
	
	private ScheduleDescription sd;
	private double dmRatio;
	
	private Map<Integer, Map<Integer, Double>> euId_mdf = new HashMap<>();
	
	private Map<Integer, Integer> stageid_accesses = new HashMap<>();
	private Map<Integer, Long> stageid_size = new HashMap<>(); // proportional to #nodes
	private Map<Integer, Long> stageid_cost = new HashMap<>(); // proportional to #nodes
	private Map<Integer, Double> stageid_ratio_inmem = new HashMap<>(); // proportional to #nodes
	
	private Map<Integer, Integer> dataset_access_count = new HashMap<>();
	private Map<Integer, Integer> dataset_expected_count = new HashMap<>();
	private Map<Integer, Boolean> datasets_all_in_mem = new HashMap<>();
	
	// Metrics
	private long __totalUpdateTime = 0;
	private long __totalRankTime = 0;
		
	public MDFMemoryManagementPolicy(ScheduleDescription sd, double dmRatio) {
		this.sd = sd;
		this.dmRatio = dmRatio;
		computeAccesses(sd);
	}
	
	@Override
	public void updateDatasetsForNode(int euId, DatasetMetadataPackage datasetsMetadata, int stageId) {
		long start = System.currentTimeMillis();
		if(! euId_mdf.containsKey(euId)) {
			euId_mdf.put(euId, new HashMap<Integer, Double>());
		}
		// Get datasets generated by this stage
		Set<DatasetMetadata> datasetsOfThisStage = new HashSet<>();
		for (DatasetMetadata dm : datasetsMetadata.newDatasets) {
			if (!dataset_expected_count.containsKey(dm.getDatasetId())) {
				dataset_expected_count.put(dm.getDatasetId(), stageid_accesses.get(stageId));
				euId_mdf.get(euId).put(dm.getDatasetId(),
						stageid_accesses.get(stageId)* Math.min(dm.getSize() * dmRatio, computeRecomputeCostFor(stageId)));
				datasetsOfThisStage.add(dm);
				//System.out.println("Stage " + stageId + ", dataset " + dm.getDatasetId() + ", accesses " + stageid_accesses.get(stageId));
			}
		}
		
		// Compute variables for the model
		long sizeOfThisDataset = computeSizeOfDataset(datasetsOfThisStage);
		long costOfDataset = computeCostOfDataset(datasetsOfThisStage);
		double percDataInMem = computeRatioDataInMem(datasetsOfThisStage, sizeOfThisDataset);
		// Store variables
		stageid_size.put(stageId, sizeOfThisDataset);
		stageid_cost.put(stageId, costOfDataset);
		stageid_ratio_inmem.put(stageId, percDataInMem);
		
		for (DatasetMetadata dm : datasetsMetadata.usedDatasets) {
			if (dataset_access_count.containsKey(dm.getDatasetId())) {
				double decrement = dataset_expected_count.get(dm.getDatasetId()) - dataset_access_count.get(dm.getDatasetId());
				decrement = (decrement-1.)/decrement;
				if (euId_mdf.get(euId).containsKey(dm.getDatasetId())) {
					euId_mdf.get(euId).put(dm.getDatasetId(), euId_mdf.get(euId).get(dm.getDatasetId())*(decrement));
				} else {
					//Source dataset. This will mark it for removal immediately, which only works because all 
					// workers run their first stages in parallel.
					dataset_expected_count.put(dm.getDatasetId(), 1);
				}
				dataset_access_count.put(dm.getDatasetId(), dataset_access_count.get(dm.getDatasetId())+1);
			} else {
				dataset_access_count.put(dm.getDatasetId(), 1);
			}
		}
				
		long end = System.currentTimeMillis();
		this.__totalUpdateTime = this.__totalUpdateTime + (end - start);
	}
	
	@Override
	public List<Integer> rankDatasetsForNode(int euId, Set<Integer> datasetIds) {
		long start = System.currentTimeMillis();
		List<Integer> rankedDatasets = new ArrayList<>();
		if(! euId_mdf.containsKey(euId)) {
			return rankedDatasets;
		}
		
		// Now we use the datasets that are alive to prune the datasets in the node
		removeEvictedDatasets(euId, datasetIds);
		
		// Evict datasets based on access patterns
		removeFinishedDatasets(euId);
		
		// We get the datasets in the node, after pruning
		Map<Integer, Double> datasetId_timestamp = euId_mdf.get(euId);
		
		Map<Integer, Double> sorted = sortByValue(datasetId_timestamp);
		System.out.println("MDF VALUES");
		for(Double v : sorted.values()) {
			System.out.print(v+" - ");
		}
		System.out.println();
		
		// TODO: may break ordering due to keyset returning a set ?
		for(Integer key : sorted.keySet()) {
			rankedDatasets.add(key);
		}
		long end = System.currentTimeMillis();
		this.__totalRankTime = this.__totalRankTime + (end - start);
		return rankedDatasets;
	}
	
	private double computeRecomputeCostFor(int stageId) {
		double recomputeCost = Long.MAX_VALUE;
		Stage s = sd.getStageWithId(stageId);
		Set<Stage> upstream = s.getDependencies();
		if(upstream.size() > 1) {
			LOG.error("upstream of more than 1 when computing recompute cost for stageId: {}" ,stageId);
		}
		if(! upstream.iterator().hasNext()) {
			return recomputeCost; // source stage, cut
		}
		int sid = upstream.iterator().next().getStageId();
		if(! stageid_size.containsKey(sid)) {
			return recomputeCost; // make sure this is not selected, as it does not exist yet
		}
		long size = stageid_size.get(sid);
		long cost = stageid_cost.get(sid);
		
		double percDataInMem = stageid_ratio_inmem.get(sid);
		
		recomputeCost = cost + percDataInMem * (size * dmRatio);
		
		return recomputeCost;
	}
	
	private long computeSizeOfDataset(Set<DatasetMetadata> datasetsMetadata) {
		long size = 0;
		for(DatasetMetadata dm : datasetsMetadata) {
			size = size + dm.getSize();
		}
		return size;
	}
	
	private long computeCostOfDataset(Set<DatasetMetadata> datasetsMetadata) {
		long cost = 0;
		for(DatasetMetadata dm : datasetsMetadata) {
			cost = cost + dm.getCreationCost();
		}
		return cost;
	}
	
	private double computeRatioDataInMem(Set<DatasetMetadata> datasetsMetadata, long sizeOfThisDataset) {
		// doing this on actual size in case datasets are of different lenghts in the future
		double r = 0;
		long mem = 0;
		for(DatasetMetadata dm : datasetsMetadata) {
			if ( !datasets_all_in_mem.containsKey(dm.getDatasetId()) ) {
				datasets_all_in_mem.put(dm.getDatasetId(), true);
			}
			if(dm.isInMem()) {
				mem = mem + dm.getSize();
			} else {
				datasets_all_in_mem.replace(dm.getDatasetId(), false);
			}
		}
		sizeOfThisDataset++;
		r = mem/sizeOfThisDataset;
		return r;
	}
	
	private void removeFinishedDatasets(int euId) {		
		// Datasets to evict		
		Set<Integer> toEvict = new HashSet<>();
		
		// Check those datasets that must be evicted
		for(Integer e : dataset_access_count.keySet()) {
			if (dataset_access_count.get(e) >= dataset_expected_count.get(e)) {
				toEvict.add(e);
			}
		}
		
		for(Integer e : toEvict) {
			dataset_access_count.remove(e);
			dataset_expected_count.remove(e);
		}
		
		// Now evict them from the data structure for propagation to the cluster
		for(Entry<Integer, Map<Integer, Double>> e : euId_mdf.entrySet()) {
			Map<Integer, Double> entry = e.getValue(); 
			for(Integer kill : toEvict) {
				entry.remove(kill);
			}
		}
	}
	
	private void computeAccesses(ScheduleDescription sd) {
		// TODO: will work if there is only one (logical) source
		for(Stage s : sd.getStages()) {
			int sid = s.getStageId();
			int numDownstream = s.getDependants().size();
			stageid_accesses.put(sid, numDownstream);
		}
	}
	
	private void removeEvictedDatasets(int euId, Set<Integer> datasetIdsToKeep) {
		Map<Integer, Double> allEntries = euId_mdf.get(euId);
		
		// Select entries to remove
		Set<Integer> toRemove = new HashSet<>();
		for(int id : allEntries.keySet()) {
			if(! datasetIdsToKeep.contains(id)) {
				toRemove.add(id);
			}
		}
		
		// Remove the selection
		for(int toRem : toRemove) {
			allEntries.remove(toRem);
		}
		
		// Update the info
		euId_mdf.put(euId, allEntries);
	}
	
	private Map<Integer, Double> sortByValue( Map<Integer, Double> map ) {
		Map<Integer, Double> result = new LinkedHashMap<>();
		Stream <Entry<Integer, Double>> st = map.entrySet().stream();
		// FIXME: precedence means that higher should be higher in ranked values. Probably need to multiply * -1 down there, or invert the order
		st.sorted(Comparator.comparingDouble(e -> (-1*e.getValue()))).forEachOrdered(e -> result.put(e.getKey(),e.getValue()));
		return result;
	}

	@Override
	public long __totalUpdateTime() {
		return this.__totalUpdateTime;
	}

	@Override
	public long __totalRankTime() {
		return this.__totalRankTime;
	}
	
	public boolean datasetIsAllInMem (DataReference ref) {
		if (datasets_all_in_mem.containsKey(ref.getId())) {
			return datasets_all_in_mem.get(ref.getId());
		}
		return true;
	}

}

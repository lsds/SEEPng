package uk.ac.imperial.lsds.seep.scheduler;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.infrastructure.ControlEndPoint;
import uk.ac.imperial.lsds.seep.infrastructure.SeepEndPoint;
import uk.ac.imperial.lsds.seep.util.Utils;


public class Stage {

	private final int stageId;
	/**
	 * Indicates the location where the stage *should* execute. 
	 * Maybe a scheduler can overwrite this info
	 */
	private ControlEndPoint location;
	
	/**
	 * A stage can be of type source (first to execute), unique (only one in the schedule)
	 * or a sink stage, that will output results.
	 */
	private StageType type;
	/**
	 * Dependencies of this stage
	 */
	private Set<Stage> upstream;
	/**
	 * This stage is a dependency of downstream stages
	 */
	private Set<Stage> downstream;
	/**
	 * All input data references this stage must consume. These will be partitioned across available tasks
	 */
	private Map<Integer, Set<DataReference>> inputDataReferences;
	/**
	 * All data references produced by this stage
	 */
	private Map<Integer, Set<DataReference>> outputDataReferences;
	/**
	 * The logical operators that are part of this stage.
	 */
	private Deque<Integer> wrapping;
	
	/**
	 * Whether this stage will create a partitioned output or not
	 */
	private boolean hasPartitionedState = false;
	private boolean hasMultipleInput = false;
	
	StageStatus status = StageStatus.WAITING;
	
	public Stage(int stageId) {
		this.stageId = stageId;
		this.upstream = new HashSet<>();
		this.downstream = new HashSet<>();
		this.inputDataReferences = new HashMap<>();
		this.outputDataReferences = new HashMap<>();
		this.wrapping = new ArrayDeque<>();
	}
	
	public Stage() { 
		this.stageId = 0;
	}
	
	public int getStageId(){
		return stageId;
	}
	
	public ControlEndPoint getStageLocation() {
		return location;
	}
	
	public void add(int opId){
		this.wrapping.push(opId);
	}
	
	public Deque<Integer> getWrappedOperators() {
		return wrapping;
	}
	
	public Map<Integer, Set<DataReference>> getInputDataReferences() {
		return inputDataReferences;
	}
	
	public void addInputDataReference(int streamId, Set<DataReference> dataReferences) {
		if(! this.inputDataReferences.containsKey(streamId)){
			this.inputDataReferences.put(streamId, new HashSet<>());
		}
		this.inputDataReferences.get(streamId).addAll(dataReferences);
	}
	
	public Map<Integer, Set<DataReference>> getOutputDataReferences() {
		return outputDataReferences;
	}
	
	public Set<SeepEndPoint> getInvolvedNodes() {
		// FIXME: cannot depend on DR, as these can be external, i.e. no endpoint inside
		// FIXME: NEW -> what about location attribute?
		Set<SeepEndPoint> in = new HashSet<>();
		for(Set<DataReference> drs : inputDataReferences.values()) {
			for(DataReference dr : drs) {
				in.add(dr.getControlEndPoint());
			}
		}
		return in;
	}
	
	public boolean responsibleFor(int opId) {
		return wrapping.contains(opId);
	}
	
	public int getIdOfOperatorBoundingStage(){
		return wrapping.peek();
	}
	
	public void setHasPartitionedState(){
		this.hasPartitionedState = true;
	}
	
	public boolean hasPartitionedState(){
		return hasPartitionedState;
	}
	
	public boolean hasDependantWithPartitionedStage() {
		for(Stage s : this.downstream) {
			if (s.hasPartitionedState()) {
				return true;
			}
		}
		return false;
	}
	
	public void setRequiresMultipleInput(){
		this.hasMultipleInput = true;
	}
	
	public boolean hasMultipleInput(){
		return hasMultipleInput;
	}
	
	public void setStageLocation(ControlEndPoint location) {
		this.location = location;
	}
	
	public void setStageType(StageType type){
		this.type = type;
	}
	
	public StageType getStageType(){
		return type;
	}
	
	public void dependsOn(Stage stage) {
		upstream.add(stage);
		stage.downstream.add(this);
	}
	
	public Set<Stage> getDependencies(){
		return upstream;
	}
	
	public Set<Stage> getDependants() {
		return downstream;
	}
	
	@Override
	public int hashCode(){
		return stageId;
	}
	
	@Override
	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append("StageId: "+stageId);
		sb.append(Utils.NL);
		sb.append("Type: "+type.toString());
		sb.append(Utils.NL);
		sb.append("Wraps these operators: ");
		sb.append(Utils.NL);
		for(Integer opId : wrapping){
			sb.append("  op -> "+opId);
			sb.append(Utils.NL);
		}
		sb.append(Utils.NL);
		sb.append("DependsOn: ");
		sb.append(Utils.NL);
		for(Stage s : upstream){
			sb.append("  st -> "+s.stageId);
			sb.append(Utils.NL);
		}
		sb.append("Serves: ");
		sb.append(Utils.NL);
		for(Stage s : downstream){
			sb.append("  st -> "+s.stageId);
			sb.append(Utils.NL);
		}
		return sb.toString();
	}
	
	public void setOutputDataReferences (Map<Integer, Set<DataReference>> newOutputDataReferences) {
		outputDataReferences = newOutputDataReferences;
	}
	
	public StageStatus getStatus() {
		return status;
	}
	
	public void setWaiting() {
		status = StageStatus.WAITING;
	}
	
	public void setReady() {
		status = StageStatus.READY;
	}
	
	public void setRunning() {
		status = StageStatus.RUNNING;
	}
	
	public void setFinished() {
		status = StageStatus.FINISHED;
	}
	
}

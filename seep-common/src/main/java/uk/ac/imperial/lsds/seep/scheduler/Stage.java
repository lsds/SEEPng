package uk.ac.imperial.lsds.seep.scheduler;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;
import uk.ac.imperial.lsds.seep.util.Utils;


public class Stage {

	private final int stageId;
	
	private StageType type;
	private Set<Stage> upstream;
	private Set<Stage> downstream;
	private Set<DataReference> inputDataReferences;
	private Set<DataReference> outputDataReferences;
	private Deque<Integer> wrapping;
	
	private boolean hasPartitionedState = false;
	private boolean hasMultipleInput = false;
	
	public Stage(int stageId){
		this.stageId = stageId;
		this.upstream = new HashSet<>();
		this.downstream = new HashSet<>();
		this.inputDataReferences = new HashSet<>();
		this.outputDataReferences = new HashSet<>();
		this.wrapping = new ArrayDeque<>();
	}
	
	public int getStageId(){
		return stageId;
	}
	
	public void add(int opId){
		this.wrapping.push(opId);
	}
	
	public Deque<Integer> getWrappedOperators() {
		return wrapping;
	}
	
	public Set<DataReference> getInputDataReferences() {
		return inputDataReferences;
	}
	
	public void addInputDataReference(Set<DataReference> dataReferences) {
		this.inputDataReferences.addAll(dataReferences);
	}
	
	public Set<DataReference> getOutputDataReferences() {
		return outputDataReferences;
	}
	
	public Set<EndPoint> getInvolvedNodes() {
		Set<EndPoint> in = new HashSet<>();
		for(DataReference dr : inputDataReferences) {
			in.add(dr.getEndPoint());
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
	
	public boolean hasPartitionedStage(){
		return hasPartitionedState;
	}
	
	public void setRequiresMultipleInput(){
		this.hasMultipleInput = true;
	}
	
	public boolean hasMultipleInput(){
		return hasMultipleInput;
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
	
	public void registerProducedResults() {
		
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
	
}

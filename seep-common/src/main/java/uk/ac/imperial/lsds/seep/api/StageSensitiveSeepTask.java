package uk.ac.imperial.lsds.seep.api;


public interface StageSensitiveSeepTask extends SeepTask {
	
	public void setStage(Integer newStageId);
	
}

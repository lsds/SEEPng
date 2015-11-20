package uk.ac.imperial.lsds.seepmaster.infrastructure.master;

public enum InfrastructureType {
	PHYSICAL_CLUSTER(0), YARN_CLUSTER(1), LOCAL_MULTITHREAD(2), DOCKER_CLUSTER(3), VIRTUAL_CLUSTER(4), SHARED_PHYSICAL_CLUSTER(5);
	
	private int type;
	
	InfrastructureType(int type){
		this.type = type;
	}
	
	public int ofType(){
		return type;
	}
	
}

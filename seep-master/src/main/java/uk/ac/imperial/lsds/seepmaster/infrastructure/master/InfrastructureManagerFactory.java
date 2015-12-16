package uk.ac.imperial.lsds.seepmaster.infrastructure.master;

public class InfrastructureManagerFactory {

	public static String nameInfrastructureManagerWithType(int infType){
		String name = null;
		if(infType == InfrastructureType.PHYSICAL_CLUSTER.ofType()) {
			name = InfrastructureType.PHYSICAL_CLUSTER.name();
		}
		else if (infType == InfrastructureType.MULTITHREAD.ofType()) {
			name = InfrastructureType.MULTITHREAD.name();
		}
		return name;
	}
	
	public static InfrastructureManager createInfrastructureManager(int infType){
		if(infType == InfrastructureType.PHYSICAL_CLUSTER.ofType()) {
			return new PhysicalClusterManager();
		}
		else if(infType == InfrastructureType.MULTITHREAD.ofType()){
			return new ThreadNodeManager();
		}
		return null;
	}
	
}

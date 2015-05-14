package uk.ac.imperial.lsds.seep.comm.protocol;

import java.util.Map;
import java.util.Set;

import uk.ac.imperial.lsds.seep.api.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;

public class ProtocolCommandFactory {
	
	public static MasterWorkerCommand buildBootstrapCommand(String ip, int port, int dataPort){
		BootstrapCommand bc = new BootstrapCommand(ip, port, dataPort);
		MasterWorkerCommand c = new MasterWorkerCommand(bc);
		return c;
	}
	
	public static MasterWorkerCommand buildCodeCommand(byte[] data, String baseClassName, String[] queryConfig, String methodName){
		CodeCommand cc = new CodeCommand(data, baseClassName, queryConfig, methodName);
		MasterWorkerCommand c = new MasterWorkerCommand(cc);
		return c;
	}
	
	public static MasterWorkerCommand buildStartQueryCommand(){
		StartQueryCommand sqc = new StartQueryCommand();
		MasterWorkerCommand c = new MasterWorkerCommand(sqc);
		return c;
	}
	
	public static MasterWorkerCommand buildStopQueryCommand(){
		StopQueryCommand sqc = new StopQueryCommand();
		MasterWorkerCommand c = new MasterWorkerCommand(sqc);
		return c;
	}
	
	public static MasterWorkerCommand buildDeadWorkerCommand(int workerId, String reason){
		DeadWorkerCommand dwc = new DeadWorkerCommand(workerId, reason);
		MasterWorkerCommand c = new MasterWorkerCommand(dwc);
		return c;
	}

	public static MasterWorkerCommand buildScheduleDeployCommand(SeepLogicalQuery slq, ScheduleDescription scheduleDescription,
			Set<EndPoint> endpoints) {
		ScheduleDeployCommand sdc = new ScheduleDeployCommand(slq, scheduleDescription, endpoints);
		MasterWorkerCommand c = new MasterWorkerCommand(sdc);
		return c;
	}

	public static MasterWorkerCommand buildExecuteStageCommand(int stageId) {
		ScheduleStageCommand sdc = new ScheduleStageCommand(stageId);
		MasterWorkerCommand c = new MasterWorkerCommand(sdc);
		return c;
	}
	
	public static MasterWorkerCommand buildMaterializeTaskCommand(Map<Integer, EndPoint> opToEndpointMapping) {
		MaterializeTaskCommand mtc = new MaterializeTaskCommand(opToEndpointMapping);
		MasterWorkerCommand c = new MasterWorkerCommand(mtc);
		return c;
	}
	
}

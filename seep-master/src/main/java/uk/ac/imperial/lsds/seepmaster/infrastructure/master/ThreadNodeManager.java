package uk.ac.imperial.lsds.seepmaster.infrastructure.master;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import joptsimple.OptionParser;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.config.CommandLineArgs;
import uk.ac.imperial.lsds.seep.config.ConfigKey;
import uk.ac.imperial.lsds.seep.infrastructure.ExecutionUnitType;
import uk.ac.imperial.lsds.seep.util.Utils;
import uk.ac.imperial.lsds.seepworker.Main;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.WorkerThread;

public class ThreadNodeManager implements InfrastructureManager {
	
	final private Logger LOG = LoggerFactory.getLogger(ThreadNodeManager.class);
	public final ExecutionUnitType executionUnitType = ExecutionUnitType.THREAD_NODE;
	private ExecutorService executorPool;
	private Deque<ExecutionUnit> threadNodes;
	private Map<Integer, Connection> connectionstoThreadNodes;
	private int currDataPort = 4600;
	private int currWorkerPort = 3600;

	public ThreadNodeManager(){
		this.threadNodes = new ArrayDeque<>();
		this.connectionstoThreadNodes = new HashMap<>();
	}
	
	@Override
	public ExecutionUnit buildExecutionUnit(InetAddress ip, int port, int dataPort) {
		return new ThreadNode(ip, port, dataPort);
	}
	
	@Override
	public void addExecutionUnit(ExecutionUnit eu) {
		threadNodes.push(eu);
		connectionstoThreadNodes.put(eu.getId(), new Connection(eu.getEndPoint()));
	}
	
	@Override
	public ExecutionUnit getExecutionUnit(){
		LOG.debug("ExecutionUnit DEQUEUE: "+ threadNodes.peek());
		
		if(threadNodes.size() > 0){
			LOG.debug("Returning 1 executionUnit, remaining: {}", threadNodes.size()-1);
			return threadNodes.pop();
		}
		else{
			LOG.error("No available executionUnits !!!");
			return null;
		}
	}

	@Override
	public boolean removeExecutionUnit(int id) {
		for(ExecutionUnit eu : threadNodes){
			if(eu.getId() == id){
				boolean success = threadNodes.remove(eu);
				if(success){
					LOG.info("ExecutionUnit id: {} was removed");
					return true;
				}
			}
		}
		return false;
	}

	@Override
	public int executionUnitsAvailable() {
		return threadNodes.size();
	}

	@Override
	public void claimExecutionUnits(int numExecutionUnits) {
		//Initialise the Worker threadPool
		this.executorPool = Executors.newFixedThreadPool(numExecutionUnits);
		
		for(int i = numExecutionUnits; i > 0 ; i--){
			//Need a more elegant way to get Master's configuration
			HashMap<String,Object> map = new HashMap<>();
			map.put("master.ip", "127.0.0.1");
			map.put("master.port", "3500");
			map.put("data.port", this.currDataPort);
			map.put("worker.port", this.currWorkerPort);

			
			// Get default properties defined in the WorkerConfig file
			List<ConfigKey> configKeys = WorkerConfig.getAllConfigKey();
			OptionParser parser = new OptionParser();
			CommandLineArgs cla = new CommandLineArgs(new String[0], parser, configKeys);
			Properties commandLineProperties = cla.getProperties();
	
			// Custom thread properties
			Properties fileProperties = new Properties();
			fileProperties.putAll(map);
	
			Properties validatedProperties = Utils.overwriteSecondPropertiesWithFirst(fileProperties,
					commandLineProperties);
			
			WorkerConfig wc = new WorkerConfig(validatedProperties);
			
			Runnable threadWorker = new WorkerThread(wc);
			this.executorPool.execute(threadWorker);
			//this.addExecutionUnit( buildExecutionUnit(InetAddress.getLoopbackAddress(), currWorkerPort,  currDataPort));
			
			this.currDataPort+=100;
			this.currWorkerPort+=100;
		}
	}

	@Override
	public void decommisionExecutionUnits(int numExecutionUnits) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void decommisionExecutionUnit(ExecutionUnit eu) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Set<Connection> getConnectionsTo(Set<Integer> executionUnitIds) {
		Set<Connection> cs = new HashSet<>();
		for(Integer id : executionUnitIds) {
			// TODO: check that the conn actually exists
			cs.add(connectionstoThreadNodes.get(id));
		}
		return cs;
	}

	@Override
	public Connection getConnectionTo(int executionUnitId) {
		return connectionstoThreadNodes.get(executionUnitId);
	}

}
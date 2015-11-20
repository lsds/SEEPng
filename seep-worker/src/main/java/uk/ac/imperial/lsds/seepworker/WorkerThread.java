package uk.ac.imperial.lsds.seepworker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerThread implements Runnable{
	
	final private static Logger LOG = LoggerFactory.getLogger(WorkerThread.class);
	private WorkerConfig wc;
	private Main workerInstance;
	
	public WorkerThread(WorkerConfig wc){
		this.wc=wc;
		this.workerInstance = new Main();
	}
	
	@Override
	public void run() {
		
		LOG.info("Starting Seep Worker as a Thread");
		this.workerInstance.executeWorker(this.wc);

	}
}

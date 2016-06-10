package uk.ac.imperial.lsds.seepworker.core;

import static com.codahale.metrics.MetricRegistry.name;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.state.SeepState;
import uk.ac.imperial.lsds.seep.core.InputAdapter;
import uk.ac.imperial.lsds.seep.core.InputAdapterReturnType;
import uk.ac.imperial.lsds.seep.metrics.SeepMetrics;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.core.Conductor.ConductorCallback;
import uk.ac.imperial.lsds.seepworker.core.input.CoreInput;
import uk.ac.imperial.lsds.seepworker.core.output.CoreOutput;

import com.codahale.metrics.Meter;

public class SingleThreadProcessingEngine implements ProcessingEngine {

	final private Logger LOG = LoggerFactory.getLogger(SingleThreadProcessingEngine.class.getName());
	final private int MAX_BLOCKING_TIME_PER_INPUTADAPTER_MS;
	
	private boolean working = false;
	private Thread worker;
	private ConductorCallback callback;
	
	private int id;
	private CoreInput coreInput;
	private CoreOutput coreOutput;
	private SeepTask task;
	private SeepState state;
		
	// Metrics
	final private Meter m;
	
	public SingleThreadProcessingEngine(WorkerConfig wc, int id, SeepTask task, SeepState state, CoreInput coreInput, CoreOutput coreOutput, ConductorCallback callback) {
		this.id = id;
		this.task = task;
		this.state = state;
		this.coreInput = coreInput;
		this.coreOutput = coreOutput;
		this.callback = callback;
		this.MAX_BLOCKING_TIME_PER_INPUTADAPTER_MS = wc.getInt(WorkerConfig.MAX_WAIT_TIME_PER_INPUTADAPTER_MS);
		this.worker = new Thread(new Worker());
		this.worker.setName(this.getClass().getSimpleName());
		m = SeepMetrics.REG.meter(name(SingleThreadProcessingEngine.class, "event", "per", "sec"));
	}

	@Override
	public void start() {
		working = true;
		this.worker.start();
	}

	@Override
	public void stop() {
		if(task != null) task.close();	// to avoid nullpointer when Ctrl^c after stopping query
		working = false;
		this.closeAndCleanEngine();
	}
	
	private void closeAndCleanEngine(){
		try {
			LOG.debug("Waiting for worker thread to die...");
			worker.join();
			LOG.debug("Waiting for worker thread to die...OK");
		} 
		catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		coreInput = null;
		coreOutput = null;
		task = null;
		state = null;
	}
	
	private class Worker implements Runnable {

		@Override
		public void run() {
			List<InputAdapter> inputAdapters = coreInput.getInputAdapters();
			LOG.info("Configuring SINGLETHREAD processing engine with {} inputAdapters", inputAdapters.size());
			Iterator<InputAdapter> it = inputAdapters.iterator();
			short one = InputAdapterReturnType.ONE.ofType();
			short many = InputAdapterReturnType.MANY.ofType();
			LOG.info("Configuring SINGLETHREAD processing engine with {} outputBuffers", coreOutput.getBuffers().size());
			
			API api = new Collector(id, coreOutput);
			
			int ongoingStreams = inputAdapters.size();
			
			while(working) {
				for(int i = 0; i < inputAdapters.size(); i++) {
					InputAdapter ia = inputAdapters.get(i);
					if(ia.returnType() == one) {
						ITuple d = ia.pullDataItem(MAX_BLOCKING_TIME_PER_INPUTADAPTER_MS);
						if(d != null) {							
							task.processData(d, api);
							m.mark();
						}
						else {
							// Exhausted IA
							ongoingStreams--;
						}
					}
					else if(ia.returnType() == many) {
						List<ITuple> d = ia.pullDataItems(MAX_BLOCKING_TIME_PER_INPUTADAPTER_MS);
						if(d != null) {
							task.processDataGroup(d, api);
							m.mark();
						}
						else {
							// Exhausted IA
							ongoingStreams--;
						}
					}
					// Reset iteration before the last element
					if(i == inputAdapters.size()-1 && working) {
						i = -1;

						if(task instanceof ScheduleTask) {
							ScheduleTask st = (ScheduleTask)task;
							List<LogicalOperator> operators = st.__get_operators();
							Iterator<LogicalOperator> it2 = operators.iterator();

							while(it2.hasNext()) {
								LogicalOperator logicalOperator = it2.next();
								if (logicalOperator.getOperatorId() == 1){ //Source-op-id
									if (m.getCount() == 0) {
										while(api.hasMoreData()){
											task.processData(null, api);
											// FIXME: hack -> just exhaust the iterator to force out of the loop
											//while (it.hasNext()) it.next();
											//System.out.println("ERROR HERE, fix HACK");
											// continue;
										}
										working = false;
										//callback.notifyOk(api.getRuntimeEvents()); // notify and pass all generated runtime events
									}
								}
								//exhaust the iterator
								while (it2.hasNext()) it2.next();
															}						}
					}
					if(! callback.isContinuousTask()) {
						if(ongoingStreams == 0) {
							task.close();
							callback.notifyOk(api.getRuntimeEvents()); // notify and pass all generated runtime events
							working = false;
							i = inputAdapters.size();
						}
					}
				}
				// If there are no input adapters, assume processData contain all necessary and give null input data
				if(working){
					LOG.info("About to call processData without data. Am I a source?");
					task.processData(null, api);
					working = false; // run source only once
				}
			}
			this.closeEngine();
		}
		
		private boolean allStreamsFinished(Map<Integer, Boolean> tracker) {
			boolean finished = true;
			for(boolean f : tracker.values()) {
				finished = finished & f;
			}
			return finished;
		}
		
		private void closeEngine(){
			LOG.info("Stopping main engine thread");
		}
		
	}
}

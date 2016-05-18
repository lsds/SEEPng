package uk.ac.imperial.lsds.seepmaster.ui.web;
/*******************************************************************************
 * Copyright (c) 2016 Imperial College London
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors:
 *     Panagiotis Garefalakis (pg1712@imperial.ac.uk)
 ******************************************************************************/
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.jetty.util.MultiMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsHandler implements RestAPIRegistryEntry {

	// Structure Type <Timestamp, MetricValue>
	private Map<Long, Float> throughput;
	private Map<Long, Float> cpuUsage;
	private Map<Long, Float> memoryUsage;
	
	final private static Logger LOG = LoggerFactory.getLogger(MetricsHandler.class.getCanonicalName());
	
	public MetricsHandler() {
		throughput = new HashMap<Long, Float>();
		cpuUsage = new HashMap<Long, Float>();
		memoryUsage = new HashMap<Long, Float>();
	}
	
	public Object getAnswer(MultiMap<String> reqParameters) {
		
		/* Metric Mode - Currently supporting CPU, MEMORY, THROUGHPUT */
		Metric_Mode mode = Metric_Mode.toMode(reqParameters.get("mode").get(0));		
		LOG.debug("getAnswer()  Mode:  {}", mode);

		Map<Long, Float> metricToReturn;
		switch(mode){
			case CPU :
				metricToReturn = this.cpuUsage;
				break;
			case MEMORY :
				metricToReturn = this.memoryUsage;
				break;
			case THROUGHPUT:
				metricToReturn = this.throughput;
				break;
			default:
				metricToReturn = null;
				break;
		}
		
		List<Float> result = new ArrayList<Float>();

		/*
		 * Do we have a time window?
		 */
		if (reqParameters.containsKey("start")
				&& reqParameters.containsKey("stop")) {
			
			long start = Long.valueOf(reqParameters.getValue("start",0));
			long stop  = Long.valueOf(reqParameters.getValue("stop",0));
			
			LOG.debug("Start: {} Stop: {} Step: {}",reqParameters.getValue("start",0), reqParameters.getValue("stop",0), reqParameters.getValue("step",0));
			
			List<Long> tmpResult = new ArrayList<Long>();
			List<Long> outdated = new ArrayList<Long>();
			synchronized(metricToReturn) {
				for (Long t : metricToReturn.keySet()) {
					if (start <= t && t <= stop) 
						tmpResult.add(t);
					if (t < start) 
						outdated.add(t);
				}
				for (Long t : outdated)
					metricToReturn.remove(t);
			}
			
			/*
			 * Is there a step to consider?
			 */
//			long step = -1;
//			if (reqParameters.containsKey("step"))
//				step = Long.valueOf(reqParameters.getValue("step",0));
//			
//			if (step != -1) {
//				
//				long stepEnd = start + step;
//				int i = 0;
//				do {
//					if (i < tmpResult.size()) {
//						Long t = tmpResult.get(i);
//						if (t < stepEnd) {
//							result.add(metricToReturn.get(t));
//							while ((t < stepEnd) && i < tmpResult.size())
//								t = tmpResult.get(i++);
//						}
//						else {
//							result.add(null);
//						}
//					}
//					else 
//						result.add(null);
//					
//					stepEnd += step;
//				}
//				while (stepEnd <= stop);
//			}
//			else {
				for (Long t : tmpResult)
					result.add(metricToReturn.get(t)); 
//			}
//		}
//		else {
//			/*
//			 * No time window
//			 */
//			synchronized(metricToReturn) {
//				result.addAll(metricToReturn.values());
//				metricToReturn.clear();
//			}
		}

		/*################################################################ 
		result.clear();
		long start = Long.valueOf(reqParameters.getValue("start",0));
		long stop  = Long.valueOf(reqParameters.getValue("stop",0));
		long step  = Long.valueOf(reqParameters.getValue("step",0));
		while ((start += step) < stop)
			result.add((float)Math.random()*10);
		/*################################################################*/
		
		return result;
	}
	
	@Override
	public Object getPostAnswer(Map<String, String[]> reqParameters) {
		/* Metric Mode - Currently supporting CPU, MEMORY, THROUGHPUT */
		Metric_Mode mode = Metric_Mode.toMode(reqParameters.get("mode")[0]);		
		String value = reqParameters.get("value")[0];
		
		LOG.debug("getPostAnswer()  Mode:  {} Value: {}", mode, value);
		
		Map<Long, Float> metricMode;
		switch(mode){
			case CPU :
				metricMode = this.cpuUsage;
				break;
			case MEMORY :
				metricMode = this.memoryUsage;
				break;
			case THROUGHPUT:
				metricMode = this.throughput;
				break;
			default:
				metricMode = null;
				break;
		}
		metricMode.put(System.currentTimeMillis(), Float.valueOf(value));
		
		return "OK";
	}
}

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

import uk.ac.imperial.lsds.seep.api.operator.DownstreamConnection;
import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;

public class QueryHandler implements RestAPIRegistryEntry {

	private SeepLogicalQuery slq;
	private List<Object> nodes = new ArrayList<Object>();
	private List<Object> edges = new ArrayList<Object>();
	private Map<String, Object> qpInformation = new HashMap<String, Object>();
	
	final private static Logger LOG = LoggerFactory.getLogger(QueryHandler.class.getCanonicalName());

	//Store query operators 
	public QueryHandler(SeepLogicalQuery slq) {
		this.slq = slq;
		this.initNodesInformation();
		this.initEdgesInformation();
	}
	
	public void initNodesInformation(){
		List<LogicalOperator> sources = this.slq.getSources();
		LogicalOperator sink = this.slq.getSink();
		
		/*
		 * Sources
		 */
		for(LogicalOperator lo: sources){
			Map<String, Object> sourceDetails = new HashMap<String, Object>();
			sourceDetails.put("id", "src" + lo.getOperatorId());
			sourceDetails.put("type", "graph_type_source");
			sourceDetails.put("name", "Data Stream Source");
			sourceDetails.put("query", "Data Stream Source");
			Map<String, Object> nData = new HashMap<String, Object>();
			nData.put("data", sourceDetails);
			nodes.add(nData);
		}
		
		
		/*
		 * Processors
		 */
		for(LogicalOperator op : this.slq.getAllOperators()){
			if(sink.getOperatorId() == op.getOperatorId())
				continue;
			boolean valid = true;
			for (LogicalOperator s : sources) {
				if (s.getOperatorId() == op.getOperatorId()){
					valid = false;
					break;
				}
			}
			if(valid){
				Map<String, Object> procesorDetails = new HashMap<String, Object>();
				procesorDetails.put("id", "op" + op.getOperatorId());
				procesorDetails.put("type", "graph_type_query");
				procesorDetails.put("name", "SEEP Processor "+op.getOperatorId());
//				procesorDetails.put("query", "SELECT AVG(att) <br />FROM stream [ROW 1]");
				procesorDetails.put("query", "Operator "+op.getSeepTask().getClass());
				Map<String, Object> nData = new HashMap<String, Object>();
				nData.put("data", procesorDetails);
				nodes.add(nData);
			}
		}
		
		/*
		 * Sink 
		 */
		Map<String, Object> sinkDetails = new HashMap<String, Object>();
		sinkDetails.put("id", "snk" + sink.getOperatorId());
		sinkDetails.put("type", "graph_type_sink");
		sinkDetails.put("name", "Data Stream Sink");
		sinkDetails.put("query", "Data Stream Sink");
		Map<String, Object> nData = new HashMap<String, Object>();
		nData.put("data", sinkDetails);
		nodes.add(nData);
		
		/*
		 * State
		 */
		for(LogicalOperator op : this.slq.getAllOperators()){
			if(op.getState() != null){
				Map<String, Object> stateDetails = new HashMap<String, Object>();
				stateDetails.put("id", "state" + op.getState().getClass());
				stateDetails.put("type", "graph_type_state");
				stateDetails.put("name", "SEEP State");
				stateDetails.put("query", "Mutable SeepState");
				Map<String, Object> stateData = new HashMap<String, Object>();
				stateData.put("data", stateDetails);
				nodes.add(stateData);
			}
		}
		
	}
	public void initEdgesInformation(){
		int StreamID = 1;
		List<LogicalOperator> sources = this.slq.getSources();
		LogicalOperator sink = this.slq.getSink();
		
		/*
		 * Sources
		 */
		for(LogicalOperator lo : sources){
			for( DownstreamConnection down: lo.downstreamConnections()){
				Map<String, Object> eDetails = new HashMap<String, Object>();
				eDetails.put("streamid", "e" + StreamID++);
				eDetails.put("source", "src"+lo.getOperatorId());
				if(down.getDownstreamOperator().getOperatorId() != sink.getOperatorId())
					eDetails.put("target", "op"+down.getDownstreamOperator().getOperatorId());
				else
					eDetails.put("target", "snk"+down.getDownstreamOperator().getOperatorId());
				eDetails.put("type", "graph_edge_defaults");
				Map<String, Object> eData = new HashMap<String, Object>();
				eData.put("data", eDetails);
				edges.add(eData);
			}
		}
		/*
		 * Processors
		 */
		for(LogicalOperator op : this.slq.getAllOperators()){
			if(sink.getOperatorId() == op.getOperatorId())
				continue;
			
			boolean valid = true;
			for (LogicalOperator s : sources) {
				if (s.getOperatorId() == op.getOperatorId())
					valid = false;
					break;
			}
			if(valid){
				for (DownstreamConnection down : op.downstreamConnections()) {
					Map<String, Object> eDetails = new HashMap<String, Object>();
					eDetails.put("streamid", "e" + StreamID++);
					eDetails.put("source", "op" + op.getOperatorId());
					if (down.getDownstreamOperator().getOperatorId() != sink.getOperatorId())
						eDetails.put("target", "op" + down.getDownstreamOperator().getOperatorId());
					else
						eDetails.put("target", "snk" + down.getDownstreamOperator().getOperatorId());
					eDetails.put("type", "graph_edge_defaults");
					Map<String, Object> eData = new HashMap<String, Object>();
					eData.put("data", eDetails);
					edges.add(eData);
				}
			}	
		}
		
		/*
		 * State
		 */
		for(LogicalOperator op : this.slq.getAllOperators()){
			if(op.getState() != null){
				Map<String, Object> eDetails = new HashMap<String, Object>();
				eDetails.put("streamid", "e" + StreamID++);
				eDetails.put("source", "state"+op.getState().getClass());
				eDetails.put("target", "op"+op.getOperatorId());
				eDetails.put("type", "graph_edge_defaults");
				Map<String, Object> eData = new HashMap<String, Object>();
				eData.put("data", eDetails);
				edges.add(eData);
			}
		}
	}
	
	@Override
	public Object getAnswer(MultiMap<String> reqParameters) {
		LOG.debug("getAnswer() Params: {} ", reqParameters.toString());
		qpInformation.put("nodes", nodes);
		qpInformation.put("edges", edges);
		return qpInformation;	
	}

	@Override
	public Object getPostAnswer(Map<String, String[]> reqParameters) {
		// TODO Auto-generated method stub
		return null;
	}
}

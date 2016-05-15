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

public class QueriesHandler implements RestAPIRegistryEntry {

	//Store query operators 
	public QueriesHandler() {
	}
	
	public Object getAnswer(MultiMap<String> reqParameters) {
		
		System.out.println("QueriesHandler.getAnswer() Params: " + reqParameters.toString());
		
		Map<String, Object> qpInformation = new HashMap<String, Object>();

		List<Object> nodes = new ArrayList<Object>();
		List<Object> edges = new ArrayList<Object>();

		//dummy global ID 
		int id = 0;
		/*
		 * Source
		 */
		Map<String, Object> sourceDetails = new HashMap<String, Object>();
		sourceDetails.put("id", "src" + id++);
		sourceDetails.put("type", "graph_type_source");
		sourceDetails.put("name", "Data Stream Source");
		sourceDetails.put("query", "Data Stream Source");
		Map<String, Object> nData = new HashMap<String, Object>();
		nData.put("data", sourceDetails);
		nodes.add(nData);
		
		/*
		 * Processor 1
		 */
		Map<String, Object> procesorOneDetails = new HashMap<String, Object>();
		procesorOneDetails.put("id", "op" + id++);
		procesorOneDetails.put("type", "graph_type_query");
		procesorOneDetails.put("name", "SEEP Processor 1");
		procesorOneDetails.put("query", "SELECT AVG(att) <br />FROM stream [ROW 1]");
		nData = new HashMap<String, Object>();
		nData.put("data", procesorOneDetails);
		nodes.add(nData);

		
		/*
		 * Processor 2
		 */
		Map<String, Object> procesorTwoDetails = new HashMap<String, Object>();
		procesorTwoDetails.put("id", "op" + id++);
		procesorTwoDetails.put("type", "graph_type_query");
		procesorTwoDetails.put("name", "SEEP Processor 2");
		procesorTwoDetails.put("query", "SELECT AVG(att) <br />FROM stream [ROW 1]");
		nData = new HashMap<String, Object>();
		nData.put("data", procesorTwoDetails);
		nodes.add(nData);
		
		/*
		 * Sink 
		 */
		Map<String, Object> sinkDetails = new HashMap<String, Object>();
		sinkDetails.put("id", "snk" + id++);
		sinkDetails.put("type", "sinkDetails");
		sinkDetails.put("name", "Data Stream Sink");
		sinkDetails.put("query", "Data Stream Sink");
		nData = new HashMap<String, Object>();
		nData.put("data", sinkDetails);
		nodes.add(nData);	
		
		Map<String, Object> eDetails = new HashMap<String, Object>();
		eDetails.put("streamid", "e" + id++);
		eDetails.put("source", "src0");
		eDetails.put("target", "op1");
		eDetails.put("type", "graph_edge_defaults");
		Map<String, Object> eData = new HashMap<String, Object>();
		eData.put("data", eDetails);
		edges.add(eData);
		
		eDetails = new HashMap<String, Object>();
		eDetails.put("streamid", "e" + id++);
		eDetails.put("source", "op1");
		eDetails.put("target", "op2");
		eDetails.put("type", "graph_edge_defaults");
		eData = new HashMap<String, Object>();
		eData.put("data", eDetails);
		edges.add(eData);
		
		eDetails = new HashMap<String, Object>();
		eDetails.put("streamid", "e" + id++);
		eDetails.put("source", "op2");
		eDetails.put("target", "snk3");
		eDetails.put("type", "graph_edge_defaults");
		eData = new HashMap<String, Object>();
		eData.put("data", eDetails);
		edges.add(eData);
		
		qpInformation.put("nodes", nodes);
		qpInformation.put("edges", edges);
		return qpInformation;
		
	}

}

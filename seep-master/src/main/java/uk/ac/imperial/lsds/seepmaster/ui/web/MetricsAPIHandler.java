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
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;
import uk.ac.imperial.lsds.seepmaster.query.GenericQueryManager;
import uk.ac.imperial.lsds.seepmaster.query.MaterializedQueryManager;


public class MetricsAPIHandler extends AbstractHandler {
	
	private GenericQueryManager qm;
	private SeepLogicalQuery slq;
	private static final ObjectMapper mapper = new ObjectMapper();
	private Map<String, RestAPIRegistryEntry> restAPIRegistry;
	
	
	final private static Logger LOG = LoggerFactory.getLogger(MetricsAPIHandler.class.getCanonicalName());
	
	public MetricsAPIHandler(GenericQueryManager qm){
		this.qm = qm;
       	this.restAPIRegistry = new HashMap<String, RestAPIRegistryEntry>();
	}
	
	
	public void configureAPI(){
		if ( ((MaterializedQueryManager)qm.getQueryManager()).getSlq() == null )
			return;
		
		this.slq = ((MaterializedQueryManager)qm.getQueryManager()).getSlq();
		List<LogicalOperator> sources = this.slq.getSources();
       	LogicalOperator sink = this.slq.getSink();
       	
		/** Configure Metrics Rest API **/
		this.restAPIRegistry = new HashMap<String, RestAPIRegistryEntry>();   	
       	restAPIRegistry.put("/metrics/snk"+sink.getOperatorId(), new MetricsHandler());
       	for(LogicalOperator s : sources){
       		restAPIRegistry.put("/metrics/src"+s.getOperatorId(), new MetricsHandler());
       	}
       	
		for(LogicalOperator op : this.slq.getAllOperators()){
			if(sink.getOperatorId() == op.getOperatorId())
				continue;
			else{
				boolean valid = true;
				for(LogicalOperator s : sources){
					if(s.getOperatorId() == op.getOperatorId()){
						valid = false;
						break;
					}
				}
				if(valid)
					restAPIRegistry.put("/metrics/op"+op.getOperatorId(), new MetricsHandler());
			}
        }
		restAPIRegistry.put("/queries", new QueryHandler(this.slq));
	}
	
	public void handle(String target, Request baseRequest,
			HttpServletRequest request, HttpServletResponse response)
			throws IOException, ServletException {
		
		String callback = request.getParameter("callback");		
		LOG.debug("Target: is: {} callback is: {}", target, callback);

		response.setContentType("application/json;charset=utf-8");
		response.setStatus(HttpServletResponse.SC_OK);
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT");
	
		
		if (!this.restAPIRegistry.containsKey(target)) {
			LOG.error("Request: {} NOT internally handled", baseRequest);
			baseRequest.setHandled(true);
			if (callback != null) 
				response.getWriter().println(callback + "(" + mapper.writeValueAsString(this.restAPIRegistry.keySet()) + ")");
			else 
				response.getWriter().println(mapper.writeValueAsString(this.restAPIRegistry.keySet()));
		}
		else {
			if (baseRequest.getMethod().equals("GET")) {
				baseRequest.setHandled(true);
				if (callback != null) 
					response.getWriter().println(callback + "(" + mapper.writeValueAsString(this.restAPIRegistry.get(target).getAnswer(baseRequest.getQueryParameters())) + ")");
				else 
					response.getWriter().println(mapper.writeValueAsString(this.restAPIRegistry.get(target).getAnswer(baseRequest.getQueryParameters())));
			}
			else if (baseRequest.getMethod().equals("POST")) {
				baseRequest.setHandled(true);
				if (callback != null) 
					response.getWriter().println(callback + "(" + mapper.writeValueAsString(this.restAPIRegistry.get(target).getPostAnswer(baseRequest.getParameterMap())) + ")");
				else 
					response.getWriter().println(mapper.writeValueAsString(this.restAPIRegistry.get(target).getPostAnswer(baseRequest.getParameterMap())));
			}
		}
	}
	
	public static Map<String, String> getReqParameters(String query) {
		String[] params = query.split("&");  
	    Map<String, String> map = new HashMap<String, String>();  
	    for (String param : params) {  
	        String name = param.split("=")[0];  
	        String value = param.split("=")[1];  
	        map.put(name, value);  
	    }  
	    return map;  
	}
}

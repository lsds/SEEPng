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
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.util.MultiMap;

import com.fasterxml.jackson.databind.ObjectMapper;


public class MetricsRestAPIHandler extends AbstractHandler {
	
	public static final ObjectMapper mapper = new ObjectMapper();
	
	private Map<String, RestAPIRegistryEntry> restAPIRegistry;
	
	public MetricsRestAPIHandler(Map<String, RestAPIRegistryEntry> restAPIRegistry) {
		this.restAPIRegistry = restAPIRegistry;
	}
	
	public static Map<String, String> getReqParameter(String query) {
		String[] params = query.split("&");  
	    Map<String, String> map = new HashMap<String, String>();  
	    for (String param : params) {  
	        String name = param.split("=")[0];  
	        String value = param.split("=")[1];  
	        map.put(name, value);  
	    }  
	    return map;  
	}
	
	public void handle(String target, Request baseRequest,
			HttpServletRequest request, HttpServletResponse response)
			throws IOException, ServletException {
		
		String callback = request.getParameter("callback");
		
		System.out.println(String.format("[DEBUG] RestAPIHandler target is %s callback is %s", target, callback));

		response.setContentType("application/json;charset=utf-8");
		response.setStatus(HttpServletResponse.SC_OK);
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT");
		
		String [] s = target.split("/");
		for (int i = 0; i < s.length; i++) {
			System.out.print(String.format("%s (%d chars)", s[i], s[i].length()));
			if (i < s.length - 1)
				System.out.print(", ");
		}
		System.out.println();
		
		System.out.println(baseRequest.getMethod());
		
		if (!this.restAPIRegistry.containsKey(target)) {
			System.out.println("[DEBUG] Request NOT internally handled");
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
					response.getWriter().println(callback + "(" + mapper.writeValueAsString(this.restAPIRegistry.get(target).getAnswer(baseRequest.getParameters())) + ")");
				else 
					response.getWriter().println(mapper.writeValueAsString(this.restAPIRegistry.get(target).getAnswer(baseRequest.getParameters())));
			}
			else if (baseRequest.getMethod().equals("POST")) {
				baseRequest.setHandled(true);
				if (callback != null) 
					response.getWriter().println(callback + "(" + mapper.writeValueAsString(this.restAPIRegistry.get(target).getAnswer(baseRequest.getParameters())) + ")");
				else 
					response.getWriter().println(mapper.writeValueAsString(this.restAPIRegistry.get(target).getAnswer(baseRequest.getParameters())));
			}
		}
	}
}

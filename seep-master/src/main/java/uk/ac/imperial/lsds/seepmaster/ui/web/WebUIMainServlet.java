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
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seepmaster.MasterConfig;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.InfrastructureManager;
import uk.ac.imperial.lsds.seepmaster.query.GenericQueryManager;

public class WebUIMainServlet extends HttpServlet {

	final private static Logger LOG = LoggerFactory.getLogger(WebUIMainServlet.class.getCanonicalName());
	private static final long serialVersionUID = 1L;
	
	private final int MAXIMUM_FILE_TO_HOLD_SIZE = 1024 * 1024 * 100; // 100 MB
	
	private GenericQueryManager qm;
	private MetricsAPIHandler metricsHandler;
	
	private String pathToJar;
	private String definitionClass;
	private String [] queryArgs = new String[]{};
	private short queryType = 0; // TODO: allow to specify query type here as well
	private String composeMethod = MasterConfig.COMPOSE_METHOD_NAME;

	public WebUIMainServlet(GenericQueryManager qm, MetricsAPIHandler metricsHandler){
		this.qm = qm;
		this.metricsHandler = metricsHandler;
	}
	
	@Override
	public void init(){
		
	}
	
	@Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

		System.out.println("ReQuest Type: "+ request.getMethod());
		System.out.println("ReQuest URI: "+ request.getRequestURI());
		System.out.println("Request Parameters: "+ request.getParameterMap());
		
		switch (request.getRequestURI()) {
		case "/action":
			LOG.info("Handling '/action' Request");
			//Get Request Parameters
			String[] actionIdValues = request.getParameterMap().get("actionid");
			int actionId = Integer.valueOf(actionIdValues[0]);
			boolean success = handleAction(actionId);
			if(!success){
				response.setContentType("text/html");
				response.setStatus(HttpServletResponse.SC_OK);
				response.sendRedirect("fail.html");
			}
			else{
				response.setContentType("text/html");
				response.setStatus(HttpServletResponse.SC_OK);
				response.sendRedirect("ok.html");
			}
			break;
		default:
			LOG.info("Handling Unkown Request: '{}'", request.getRequestURI());
			break;
		}
		
    }
	
	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		boolean success = false;
		// Check that we have a file upload request
		boolean isMultipart = ServletFileUpload.isMultipartContent(request);
		// sanity check
		if(!isMultipart){
			success = false;
		}
		else{
			List<FileItem> items = handleFileUploadForm(request);
			success = this.handleFileItems(items);
		}
		if(!success){
			response.setContentType("text/html");
			response.setStatus(HttpServletResponse.SC_OK);
			response.sendRedirect("fail.html");
		}
		else{
			response.setContentType("text/html");
			response.setStatus(HttpServletResponse.SC_OK);
			response.sendRedirect("ok.html");
		}
	}
	
	private boolean handleFileItems(List<FileItem> items){
		Iterator<FileItem> iter = items.iterator();
		while (iter.hasNext()) {
		    FileItem item = iter.next();

		    if (item.isFormField()) {	        
		        String name = item.getFieldName();
		        if(name.equals("baseclass")){
		        	definitionClass = item.getString();
		        }
		    } 
		    else {
		        pathToJar = processUploadedFile(item);
		    }
		}
		
		boolean success  = qm.loadQueryFromFile(this.queryType, this.pathToJar, this.definitionClass, this.queryArgs, this.composeMethod);
		this.metricsHandler.configureAPI();
		
		return success;
	}
	
	private String processUploadedFile(FileItem item){
		
		File uploadedFile = new File("queryfile_viaweb.jar");
	    try {
			item.write(uploadedFile);
		} 
	    catch (Exception e) {
			LOG.error("Write Uploaded File to Disk Exception: \n {} ", e);
		}
	    return uploadedFile.getAbsolutePath();
	}
	
	private List<FileItem> handleFileUploadForm(HttpServletRequest request){
		List<FileItem> items = null;
		// Create a factory for disk-based file items
		DiskFileItemFactory factory = new DiskFileItemFactory();
		factory.setSizeThreshold(MAXIMUM_FILE_TO_HOLD_SIZE);
		// Configure a repository (to ensure a secure temp location is used)
		ServletContext servletContext = this.getServletConfig().getServletContext();
		File repository = (File) servletContext.getAttribute("javax.servlet.context.tempdir");
		factory.setRepository(repository);

		// Create a new file upload handler
		ServletFileUpload upload = new ServletFileUpload(factory);
		upload.setSizeMax(MAXIMUM_FILE_TO_HOLD_SIZE);
		// Parse the request
		try {
			items = upload.parseRequest(request);
		} 
		catch (FileUploadException e) {
			LOG.error("Write Uploaded File to Disk Exception: \n {} ", e);
		}
		return items;
	}
	
	private boolean handleAction(int action){
		boolean allowed = false;
		
		switch(action){
		case 1:
			LOG.info("Deploying query to nodes...");
			allowed = qm.deployQueryToNodes();
			if(!allowed){
				LOG.warn("Could not deploy query");
			}
			else{
				LOG.info("Deploying query to nodes...OK");
			}
			return allowed;
		case 2:
			LOG.info("Starting query...");
			allowed = qm.startQuery();
			if(!allowed){
				LOG.warn("Could not start query");
			}
			else{
				LOG.info("Starting query...OK");
			}
			return allowed;
		case 3:
			LOG.info("Stopping query...");
			allowed = qm.stopQuery();
			if(!allowed){
				LOG.warn("Could not stop query");
			}
			else{
				LOG.info("Stopping query...OK");
			}
			return allowed;
		case 4:
			LOG.info("Exit");
			return true;
		default:
			LOG.info("Servlet Action: Unknown! "+ action);
		}
		return false;
	}

}

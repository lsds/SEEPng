package uk.ac.imperial.lsds.seepmaster.ui;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seepmaster.ui.web.MetricsHandler;
import uk.ac.imperial.lsds.seepmaster.ui.web.MetricsRestAPIHandler;
import uk.ac.imperial.lsds.seepmaster.ui.web.QueriesHandler;
import uk.ac.imperial.lsds.seepmaster.ui.web.RestAPIRegistryEntry;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.InfrastructureManager;
import uk.ac.imperial.lsds.seepmaster.query.GenericQueryManager;
import uk.ac.imperial.lsds.seepmaster.ui.web.WebUIMainServlet;

public class WebUI implements UI{

	final private static Logger LOG = LoggerFactory.getLogger(WebUI.class);
	
	/** Folder where all html, js etc. files are located **/
	private String baseDirectory = "webui";
	private WebUIMainServlet actionHandler;
	private Server jettyServer;
	
	public WebUI(GenericQueryManager qm, InfrastructureManager inf){
		actionHandler = new WebUIMainServlet(qm, inf);
		silenceJettyLogger();
		this.jettyServer = new Server(8888);
		
		/**
		 * Configure Multiple Handlers here - Accessed through Restful API 
		 * 1) Action Handler for users to upload queries ( Path: /action )
		 * 2) Metric Handler for query Metrics ( Path: /metric )
		 * 3) DefaultHandler for everything else (?)
		 */
		
		// Configure resourceHandler - used for static content 
		ResourceHandler mainHandler = new ResourceHandler();
		mainHandler.setDirectoriesListed(true);
        mainHandler.setWelcomeFiles(new String[]{ "index.html" });
        URL url = this.getClass().getClassLoader().getResource(baseDirectory);
        String basePath = url.toExternalForm();
        mainHandler.setResourceBase(basePath);
        
        LOG.info("Web resource base: {}", mainHandler.getBaseResource());
        
        /** Configure User-Actions servletHandler **/
        ServletHandler actionServletHandler = new ServletHandler();
        actionServletHandler.addServletWithMapping(new ServletHolder(actionHandler), "/action");
        
        /** Configure Metrics Rest API **/
        Map<String, RestAPIRegistryEntry> metricsRestAPIRegistry = new HashMap<String, RestAPIRegistryEntry>();
        metricsRestAPIRegistry.put("/queries", new QueriesHandler());
		// handler for source (with id=s0)
        metricsRestAPIRegistry.put("/metrics/src0", new MetricsHandler());
		// handler for first operator (with id=1)
        metricsRestAPIRegistry.put("/metrics/op1", new MetricsHandler());
		// handler for second operator (with id=2)
        metricsRestAPIRegistry.put("/metrics/op2", new MetricsHandler());
		// handler for sink (with id=3)
        metricsRestAPIRegistry.put("/metrics/snk3", new MetricsHandler());
        
        // Configure ALL handlers using a HandlerList
        HandlerList handlers = new HandlerList();
        handlers.setHandlers(new Handler[] { mainHandler, actionServletHandler, new MetricsRestAPIHandler(metricsRestAPIRegistry), new DefaultHandler() });
        jettyServer.setHandler(handlers);
        
        // Configure connector
        ServerConnector http = new ServerConnector(jettyServer);
        http.setHost("localhost");
        http.setPort(8080);
        http.setIdleTimeout(30000);
        jettyServer.addConnector(http);
	}
	
	@Override
	public void start() {
		try {
			jettyServer.start();
			LOG.info("Web UI running at: {}", jettyServer.getURI());
			jettyServer.join();
			
		} 
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}
	
	private void silenceJettyLogger(){
		final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger("org.eclipse.jetty");
		if (!(logger instanceof ch.qos.logback.classic.Logger)) {
		    return;
		}
		ch.qos.logback.classic.Logger logbackLogger = (ch.qos.logback.classic.Logger) logger;
		logbackLogger.setLevel(ch.qos.logback.classic.Level.INFO);
	}
}
package uk.ac.imperial.lsds.seepmaster.ui;

import java.net.URL;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seepmaster.infrastructure.master.InfrastructureManager;
import uk.ac.imperial.lsds.seepmaster.query.GenericQueryManager;
import uk.ac.imperial.lsds.seepmaster.ui.web.MetricsAPIHandler;
import uk.ac.imperial.lsds.seepmaster.ui.web.WebUIMainServlet;

public class WebUI implements UI{

	private Server jettyWebServer;
	private String htmlBaseDir = "webui";
	//Handlers
	private ResourceHandler staticContentHanlder;
	private WebUIMainServlet actionHandler;
	private MetricsAPIHandler metricsHandler;
	
	final private static Logger LOG = LoggerFactory.getLogger(WebUI.class);
	
	public WebUI(GenericQueryManager qm, InfrastructureManager inf){
		
		// used for static content 
		this.staticContentHanlder = new ResourceHandler();
		this.actionHandler = new WebUIMainServlet(qm, inf);
		this.metricsHandler = new MetricsAPIHandler(qm);
		
		
		this.silenceJettyLogger();
		this.jettyWebServer = new Server(8888);
		
		/**
		 * Configure a ContextHandler for EACH of the handlers
		 * 1) Action Handler for users to upload queries ( Path: /action )
		 * 2) Metric Handler for query Metrics ( Path: /metric )
		 * 3) DefaultHandler for everything else (?)
		 */
		
		staticContentHanlder.setDirectoriesListed(true);
        staticContentHanlder.setWelcomeFiles(new String[]{ "index.html" });
        URL url = this.getClass().getClassLoader().getResource(htmlBaseDir);
        String basePath = url.toExternalForm();
        staticContentHanlder.setResourceBase(basePath);
        
        LOG.info("Web resource base: {}", staticContentHanlder.getBaseResource());
        ContextHandler staticContext = new ContextHandler();
        staticContext.setContextPath("/");
        staticContext.setHandler(this.staticContentHanlder);
        
        
        /** Configure User-Actions Context Handler **/
        ServletContextHandler actionContext = new ServletContextHandler();
        actionContext.addServlet(new ServletHolder(this.actionHandler), "/action");
        
        
        ContextHandler metricsContext = new ContextHandler();
        metricsContext.setContextPath("/rest");
        metricsContext.setHandler(this.metricsHandler);
        

        ContextHandlerCollection contexts = new ContextHandlerCollection();
        contexts.setHandlers(new Handler[] { 
        	staticContext, actionContext, metricsContext
        }
        );
        
        // Configure ALL handlers using a HandlerList
//        HandlerList handlers = new HandlerList();
//        handlers.setHandlers(new Handler[] { new MetricsRestAPIHandler(metricsRestAPIRegistry), staticContentHanlder });
        jettyWebServer.setHandler(contexts);
        
        // Configure connector
//        ServerConnector http = new ServerConnector(jettyServer);
//        http.setHost("localhost");
//        http.setPort(8080);
//        http.setIdleTimeout(30000);
//        jettyServer.addConnector(http);
	}
	
	@Override
	public void start() {
		//Update API with LogicalQueryOps Before Start (if loaded)
		this.metricsHandler.configureAPI();
		
		try {
			jettyWebServer.start();
			LOG.info("Web UI running at: {}", jettyWebServer.getURI());
			jettyWebServer.join();
			
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
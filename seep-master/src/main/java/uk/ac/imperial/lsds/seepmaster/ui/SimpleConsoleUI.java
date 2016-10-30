package uk.ac.imperial.lsds.seepmaster.ui;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seepmaster.infrastructure.master.InfrastructureManager;
import uk.ac.imperial.lsds.seepmaster.query.GenericQueryManager;

public class SimpleConsoleUI implements UI {

	final private Logger LOG = LoggerFactory.getLogger(SimpleConsoleUI.class.getName());
	
	private static String uiText;
	private static String emptyText;
	
	private boolean working = false;
	
	static{
		// Build ui text
		StringBuilder sb = new StringBuilder();
		sb.append("#############");
		sb.append(System.getProperty("line.separator"));
		sb.append("SEEP Master SimpleConsole");
		sb.append(System.getProperty("line.separator"));
		sb.append("--------------");
		sb.append(System.getProperty("line.separator"));
		sb.append(System.getProperty("line.separator"));
		sb.append("Choose an option and press Enter");
		sb.append(System.getProperty("line.separator"));
		sb.append("0. Load query in Master");
		sb.append(System.getProperty("line.separator"));
		sb.append("1. Deploy query to Cluster");
		sb.append(System.getProperty("line.separator"));
		sb.append("2. Start query");
		sb.append(System.getProperty("line.separator"));
		sb.append("3. Stop query");
		sb.append(System.getProperty("line.separator"));
		sb.append("100. Exit");

		uiText = sb.toString();
		
		StringBuilder sb2 = new StringBuilder();
		sb.append(System.getProperty("line.separator"));
		sb.append(System.getProperty("line.separator"));
		sb.append(System.getProperty("line.separator"));
		sb.append(System.getProperty("line.separator"));
		sb.append(System.getProperty("line.separator"));
		sb.append(System.getProperty("line.separator"));
		sb.append(System.getProperty("line.separator"));
		
		emptyText = sb2.toString();
	}
	
	private GenericQueryManager qm;
	private InfrastructureManager inf;
	
	public SimpleConsoleUI(GenericQueryManager qm, InfrastructureManager inf){
		this.qm = qm;
		this.inf = inf;
	}
	
	@Override
	public void start() {
		working = true;
		this.consoleOutputMessage();
		LOG.info("Entering UI simpleConsole...");
		while(working){
			try{
				BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
				String option = br.readLine();
				boolean allowed = false;
				short queryType = -1;
				String pathToJar = null; 
				String definitionClass = null;
				String[] queryArgs = null;
				String composeMethod = null;
				switch(option){
				case "0":
					LOG.info("Loading query in Master...");
					String queryTypeStr = getUserInput("Input queryType, seepLogicalQuery (0) or schedule (1): ");
					queryType = Short.valueOf(queryTypeStr);
					pathToJar = getUserInput("Write absolute path to query (jar file): ");
					definitionClass = getUserInput("Write definition class name: ");
					// FIXME: allow to specify query parameters here as well
					queryArgs = new String[]{""};
					// FIXME: get rid of harcoded name
					composeMethod = "compose";
					
					allowed = qm.loadQueryFromFile(queryType, pathToJar, definitionClass, queryArgs, composeMethod, true);
					if(!allowed){
						LOG.warn("Could not load query");
						break;
					}
					LOG.info("Loading query in Master...OK");
					break;
				case "1":
					LOG.info("Deploying query to nodes...");
					allowed = qm.deployQueryToNodes();
					if(!allowed){
						LOG.warn("Could not deploy query");
						break;
					}
					LOG.info("Deploying query to nodes...OK");
					break;
				case "2":
					LOG.info("Starting query...");
					allowed = qm.startQuery();
					if(!allowed){
						LOG.warn("Could not start query");
						break;
					}
					LOG.info("Starting query...OK");
					break;
				case "3":
					LOG.info("Stopping query...");
					allowed = qm.stopQuery();
					if(!allowed){
						LOG.warn("Could not stop query");
						break;
					}
					LOG.info("Stopping query...OK");
					break;
				case "100":
					// FIXME: Contact workers to shut them down
					System.exit(0);
					break;
				default:
					System.out.println("NOT RECOGNIZED");
					consoleOutputMessage();
				}
			}
			catch(IOException io){
				
			}
		}
		LOG.info("Exiting UI simpleConsole...");
	}

	@Override
	public void stop() {
		this.working = false;
	}
	
	private String getUserInput(String msg) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		System.out.println(msg);
		String option = br.readLine();
		return option;
	}
	
	public void consoleOutputMessage(){
		// Shallow attempt to empty screen
		System.out.println(emptyText);
		// Print message
		System.out.println(uiText);
	}

}

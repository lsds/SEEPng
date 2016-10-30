package uk.ac.imperial.lsds.seepmaster;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import joptsimple.OptionParser;
import uk.ac.imperial.lsds.seep.comm.Comm;
import uk.ac.imperial.lsds.seep.comm.IOComm;
import uk.ac.imperial.lsds.seep.comm.serialization.JavaSerializer;
import uk.ac.imperial.lsds.seep.config.CommandLineArgs;
import uk.ac.imperial.lsds.seep.config.ConfigKey;
import uk.ac.imperial.lsds.seep.infrastructure.ControlEndPoint;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.api.RestAPIQueryManager;
import uk.ac.imperial.lsds.seep.util.Utils;
import uk.ac.imperial.lsds.seepmaster.comm.MasterWorkerAPIImplementation;
import uk.ac.imperial.lsds.seepmaster.comm.MasterWorkerCommManager;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.InfrastructureManager;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.InfrastructureManagerFactory;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.api.RestAPIMaster;
import uk.ac.imperial.lsds.seepmaster.query.GenericQueryManager;
import uk.ac.imperial.lsds.seepmaster.query.InvalidLifecycleStatusException;
import uk.ac.imperial.lsds.seepmaster.ui.UI;
import uk.ac.imperial.lsds.seepmaster.ui.UIFactory;


public class Main {
	
	final private static Logger LOG = LoggerFactory.getLogger(Main.class);

	private void executeMaster(String[] args, MasterConfig mc, String[] queryArgs) {
		int infType = mc.getInt(MasterConfig.DEPLOYMENT_TARGET_TYPE);
		LOG.info("Deploy target of type: {}", InfrastructureManagerFactory.nameInfrastructureManagerWithType(infType));
		InfrastructureManager inf = InfrastructureManagerFactory.createInfrastructureManager(infType);
		LifecycleManager lifeManager = LifecycleManager.getInstance();
		// TODO: get file from config if exists and parse it to get a map from operator to endPoint
		Map<Integer, ControlEndPoint> mapOperatorToEndPoint = null;
		// TODO: from properties get serializer and type of thread pool and resources assigned to it
		Comm comm = new IOComm(new JavaSerializer(), Executors.newCachedThreadPool());
		GenericQueryManager qm = GenericQueryManager.getInstance(inf, mapOperatorToEndPoint, comm, lifeManager, mc);
		// TODO: put this in the config manager
		int port = mc.getInt(MasterConfig.CONTROL_PORT);
		MasterWorkerAPIImplementation api = new MasterWorkerAPIImplementation(qm, inf);
		MasterWorkerCommManager mwcm = new MasterWorkerCommManager(port, api);
		mwcm.start();

		boolean enableRestAPI = mc.getString(MasterConfig.REST_API_ENABLED).equals("true");

		int uiType = mc.getInt(MasterConfig.UI_TYPE);
		UI ui = UIFactory.createUI(uiType, qm, inf);
		LOG.info("Created UI of type: {}", UIFactory.nameUIOfType(uiType));
		
		short queryType = -1;
		String queryPathFile = null;
		String baseClass = null;
		String composeMethod = null;
		// TODO: find a more appropriate way of checking whether a property is defined in config
		if(! mc.getString(MasterConfig.QUERY_FILE).equals("") && (! mc.getString(MasterConfig.BASECLASS_NAME).equals(""))){
			queryType = ((short)mc.getInt(MasterConfig.QUERY_TYPE));
			queryPathFile = mc.getString(MasterConfig.QUERY_FILE);
			baseClass = mc.getString(MasterConfig.BASECLASS_NAME);
			composeMethod = mc.getString(MasterConfig.COMPOSE_METHOD_NAME);
			LOG.info("Loading query {} with baseClass: {} from file...", queryPathFile, baseClass);
			boolean success = qm.loadQueryFromFile(queryType, queryPathFile, baseClass, queryArgs, composeMethod, enableRestAPI);
			if(! success){
				throw new InvalidLifecycleStatusException("Could not load query due to attempt to violate app lifecycle");
			}
			LOG.info("Loading query...OK");
		}

		if (enableRestAPI) {
			RestAPIMaster.RestAPIMasterManager.getInstance().addToRegistry("/queryplan", new RestAPIQueryManager(qm));
			RestAPIMaster.RestAPIMasterManager.getInstance().startServer(mc.getInt(MasterConfig.REST_API_MASTER_PORT));
		}
		
		ui.start();
	}
	
	public static void main(String args[]) {
		// Register JVM shutdown hook
		registerShutdownHook();
		// Get Properties with command line configuration 
		List<ConfigKey> configKeys = MasterConfig.getAllConfigKey();
		OptionParser parser = new OptionParser();
		// Unrecognized options are passed through to the query
		parser.allowsUnrecognizedOptions();
		CommandLineArgs cla = new CommandLineArgs(args, parser, configKeys);
		Properties commandLineProperties = cla.getProperties();
		
		// Get Properties with file configuration
		Properties fileProperties = Utils.readPropertiesFromFile(cla.getProperties().getProperty(MasterConfig.PROPERTIES_FILE), MasterConfig.PROPERTIES_RESOURCE_FILE);
		
		// Merge both properties, command line has preference
		Properties validatedProperties = Utils.overwriteSecondPropertiesWithFirst(commandLineProperties, fileProperties, configKeys);
		boolean validates = validateProperties(validatedProperties);
		if(!validates){
			printHelp(parser);
			System.exit(0);
		}
		MasterConfig mc = new MasterConfig(validatedProperties);
		Main instance = new Main();
		instance.executeMaster(args, mc, cla.getQueryArgs());
	}
	
	private static boolean validateProperties(Properties validatedProperties) {	
		return true;
	}
	
	private static void printHelp(OptionParser parser) {
		try {
			parser.printHelpOn(System.out);
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void registerShutdownHook() {
		Thread hook = new Thread(new MasterShutdownHookWorker());
		Runtime.getRuntime().addShutdownHook(hook);
	}
}
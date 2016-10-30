package uk.ac.imperial.lsds.seepmaster.infrastructure.master.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.imperial.lsds.seep.infrastructure.api.RestAPIHandler;
import uk.ac.imperial.lsds.seep.infrastructure.api.RestAPIRegistryEntry;

import org.eclipse.jetty.server.Server;

import java.util.HashMap;
import java.util.Map;

public class RestAPIMaster {

    final private static Logger LOG = LoggerFactory.getLogger(RestAPIMaster.class);

    public RestAPIMaster () {
    }

    public static class RestAPIMasterManager {

        // Only one instance is safe as schemaId is handled internally automatically
        private static RestAPIMasterManager instance = null;

        private Map<String, RestAPIRegistryEntry> restAPIRegistry;
        private Server restAPIServer;
        private int restAPIPort;

        private RestAPIMasterManager() {
            restAPIRegistry = new HashMap<>();
        }

        public static RestAPIMasterManager getInstance(){
            if(instance == null){
                instance = new RestAPIMasterManager();
            }
            return instance;
        }

        public RestAPIMasterManager addToRegistry (String location, RestAPIRegistryEntry information) {
            restAPIRegistry.put(location, information);

            return this;
        }

        public void startServer (int port) {
            restAPIPort = port;
            restAPIServer = new Server(restAPIPort);

            restAPIServer.setHandler(new RestAPIHandler(restAPIRegistry));
            try {
                restAPIServer.start();
            } catch (Exception e) {
                LOG.error("Failed to start server for restful node API:\n{}", e.getMessage());
            }
        }
    }

}


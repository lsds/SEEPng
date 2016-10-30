package uk.ac.imperial.lsds.seepmaster.infrastructure.master.api;

import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.imperial.lsds.seep.infrastructure.SeepEndPoint;
import uk.ac.imperial.lsds.seep.infrastructure.api.RestAPIHandler;
import uk.ac.imperial.lsds.seep.infrastructure.api.RestAPIRegistryEntry;

import java.util.HashMap;
import java.util.Map;

public class RestAPINodeManager {

    final private static Logger LOG = LoggerFactory.getLogger(RestAPINodeManager.class);

    private Map<String, RestAPIRegistryEntry> restAPIRegistry;
    private Server restAPIServer;
    private int restAPIPort;

    public RestAPINodeManager() {
        restAPIRegistry = new HashMap<>();
    }


    public RestAPINodeManager addToRegistry (String location, RestAPIRegistryEntry information) {
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


package uk.ac.imperial.lsds.seep.infrastructure.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by elkasoapy on 20/10/16.
 */
public class RestAPIManager {

    final private Logger LOG = LoggerFactory.getLogger(RestAPIManager.class);

    public static Map<String, RestAPIRegistryEntry> restAPIRegistry;
}

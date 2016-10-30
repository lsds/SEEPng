package uk.ac.imperial.lsds.seep.infrastructure.api;

import org.eclipse.jetty.util.MultiMap;
import uk.ac.imperial.lsds.seep.infrastructure.SeepEndPoint;

public class RestAPINodeDescription implements RestAPIRegistryEntry {

    private SeepEndPoint nodesDesc;

    public RestAPINodeDescription(SeepEndPoint nodesDesc) {
        this.nodesDesc = nodesDesc;
    }

    @Override
    public Object getAnswer(MultiMap<String> reqParameters) {
        return this.nodesDesc;
    }

}


package uk.ac.imperial.lsds.seepmaster.infrastructure.master.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.eclipse.jetty.util.MultiMap;

import uk.ac.imperial.lsds.seep.api.QueryBuilder;
import uk.ac.imperial.lsds.seep.api.operator.DownstreamConnection;
import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;

import com.fasterxml.jackson.databind.ObjectMapper;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.infrastructure.api.RestAPIRegistryEntry;
import uk.ac.imperial.lsds.seepmaster.query.QueryManager;

public class RestAPIQueryManager implements RestAPIRegistryEntry {

    //public static final ObjectMapper mapper = new ObjectMapper();

    private QueryManager queryManager;

    private Map<String, Object> process_node_lo_information(LogicalOperator lo, String type) {
        Map<String, Object> nDetails = new HashMap<>();

        nDetails.put("id", "" + lo.getOperatorId());
        nDetails.put("type", "graph_type_query");

        //if (c.getOpContext().getOperatorStaticInformation() != null) {
        //    nDetails.put("ip", c.getOpContext().getOperatorStaticInformation().getMyNode().getIp());
        //    nDetails.put("port", c.getOpContext().getOperatorStaticInformation().getMyNode().getPort());
        //}

        Map<String, Object> nData = new HashMap<String, Object>();
        nData.put("data", nDetails);

        return nData;
    }

    private List<Object> process_edges_lo_information(LogicalOperator lo) {
        List<Object> edges = new ArrayList<Object>();

        Iterator<DownstreamConnection> iter = lo.downstreamConnections().iterator();
        while (iter.hasNext()) {
            DownstreamConnection dc = iter.next();
            Map<String, Object> eDetails = new HashMap<>();
            eDetails.put("streamid", lo.getOperatorId() + "-" + dc.getStreamId());
            eDetails.put("source", "" + lo.getOperatorId());
            eDetails.put("target", "" + dc.getDownstreamOperator().getOperatorId());
            eDetails.put("type", "graph_edge_defaults");
            Map<String, Object> eData = new HashMap<String, Object>();
            eData.put("data", eDetails);
            edges.add(eData);
        }

        return edges;
    }

    public RestAPIQueryManager(QueryManager queryPlan) {
        this.queryManager = queryPlan;
    }

    @Override
    public Object getAnswer(MultiMap<String> reqParameters) {
        return queryManager.extractQueryOperatorsInformation();
    }

}

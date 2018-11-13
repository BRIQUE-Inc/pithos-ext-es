package org.chronotics.pithos.ext.es.adaptor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.chronotics.pandora.java.exception.ExceptionUtil;
import org.chronotics.pandora.java.log.Logger;
import org.chronotics.pandora.java.log.LoggerFactory;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.client.transport.TransportClient;

import java.util.HashMap;

public class ElasticCluster {
    protected Logger objLogger = LoggerFactory.getLogger(ElasticCluster.class);
    ElasticConnection objESConnection;
    TransportClient objESClient;
    ObjectMapper objMapper = new ObjectMapper();

    public ElasticCluster(ElasticConnection objESConnection) {
        this.objESConnection = objESConnection;
        this.objESClient = this.objESConnection.objESClient;
    }

    public HashMap<String, Object> healthCheckNode() {
        HashMap<String, Object> mapStatus = new HashMap<>();

        try {
            NodesStatsResponse objHealthResponse = objESClient.admin().cluster().prepareNodesStats().all().get();

            if (objHealthResponse != null && objHealthResponse.getNodes() != null && objHealthResponse.getNodes().size() > 0) {
                for (NodeStats curNodeStat : objHealthResponse.getNodes()) {
                    String strNodeId = curNodeStat.getNode().getId();

                    mapStatus.put(strNodeId, curNodeStat);
                }
            }
        } catch (Exception objEx) {
            objLogger.warn("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return mapStatus;
    }
}

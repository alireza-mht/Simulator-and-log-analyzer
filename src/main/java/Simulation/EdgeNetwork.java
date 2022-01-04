package Simulation;

import javafx.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class EdgeNetwork {
    private List<EdgeNode> edgeNodes;

//    private Map<Pair<EdgeNode, EdgeNode>,Double> bandwidth;
//    private Map<Pair<EdgeNode, EdgeNode>, Double> latency;
//

    public EdgeNetwork(List<EdgeNode> edgeNodes, Map<Pair<EdgeNode, EdgeNode>, Double> bandwidth,
                       Map<Pair<EdgeNode, EdgeNode>, Double> latency) {
        this.edgeNodes = edgeNodes;
//        this.bandwidth = bandwidth;
//        this.latency = latency;
        edgeNodes.forEach(edgeNode -> edgeNode.setEdgeNetwork(this));

    }
    public EdgeNetwork(List<EdgeNode> edgeNodes){
        this.edgeNodes = edgeNodes;
        edgeNodes.forEach(edgeNode -> edgeNode.setEdgeNetwork(this));
    }

    public double propagationDelay(EdgeNode mainEdgeNode, EdgeNode secondEdgeNode, double size) {
        double bandwidth = Math.min(mainEdgeNode.getOutgoingBandwidth(),secondEdgeNode.getIncomingBandwidth());
//        double bandwidth = this.bandwidth.entrySet().stream().filter(pairIntegerEntry ->
//                pairIntegerEntry.getKey().getKey().equals(mainEdgeNode) &&
//                        pairIntegerEntry.getKey().getValue().equals(secondEdgeNode)).findFirst().get().getValue();
        return size / bandwidth;
    }


    public List<EdgeNode> getEdgeNodes() {
        return edgeNodes;
    }

    public double getLatencyPair(EdgeNode mainEdgeNode, EdgeNode subEdgeNode) {
        return mainEdgeNode.getOutgoingLatency() + subEdgeNode.getIncomingLatency();
//        List<Pair<EdgeNode, EdgeNode>> pairList = new ArrayList<>(latency.keySet());
//        List<Pair<EdgeNode, EdgeNode>> pair = pairList.stream().filter(edgeNodeEdgeNodePair ->
//                (edgeNodeEdgeNodePair.getKey().equals(mainEdgeNode) &&
//                        edgeNodeEdgeNodePair.getValue().equals(subEdgeNode))).collect(Collectors.toList());
//        return this.latency.get(pair.get(0));
    }

    public void restEdgeNetwork() {
        this.getEdgeNodes().forEach(edgeNode -> edgeNode.setPlacedOperator(null));
    }

    public void localRestEdgeNetwork() {

    }
}

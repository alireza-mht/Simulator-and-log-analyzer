package Simulation;

import java.util.ArrayList;
import java.util.List;

public class EdgeNode {
    private String name;
    private int threadNum;
    private int id;
    private int MIPS;
    private List<EdgeNode> neighbours;
    private double incomingBandwidth;
    private double outgoingBandwidth;
    private List<Operator> placedOperators;
    private double incomingLatency;
    private double outgoingLatency;



    private EdgeNetwork edgeNetwork;

    public EdgeNode(int id, String name, int threadNum, int MIPS, double incomingBandwidth, double outgoingBandwidth,
                    double incomingLatency, double outgoingLatency) {
        this.id = id;
        this.threadNum = threadNum;
        this.name = name;
        this.MIPS = MIPS;
        this.incomingBandwidth = incomingBandwidth;
        this.outgoingBandwidth = outgoingBandwidth;
        this.neighbours = new ArrayList<>();
        this.placedOperators = new ArrayList<>();
        this.incomingLatency = incomingLatency;
        this.outgoingLatency = outgoingLatency;
    }

    public void addNeighbour(EdgeNode edgeNode) {
        neighbours.add(edgeNode);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getMIPS() {
        return MIPS;
    }

    public void setMIPS(int MIPS) {
        this.MIPS = MIPS;
    }

    public List<EdgeNode> getNeighbours() {
        return neighbours;
    }

    public void setNeighbours(List<EdgeNode> neighbours) {
        this.neighbours = neighbours;
    }

    public double getIncomingBandwidth() {
        return incomingBandwidth;
    }

    public void setIncomingBandwidth(int incomingBandwidth) {
        this.incomingBandwidth = incomingBandwidth;
    }

    public double getOutgoingBandwidth() {
        return outgoingBandwidth;
    }

    public int getId() {
        return id;
    }

    public void setOutgoingBandwidth(int outgoingBandwidth) {
        this.outgoingBandwidth = outgoingBandwidth;
    }

//    public Operator getPlacedOperator() {
//        return placedOperator;
//    }
//
    public void setPlacedOperator(Operator placedOperator) {
        this.placedOperators.add(placedOperator);
    }

    public List<Operator> getPlacedOperators() {
        return placedOperators;
    }

    public void setPlacedOperators(List<Operator> placedOperators) {
        this.placedOperators = placedOperators;
    }

    public int getThreadNum() {
        return threadNum;
    }

    public void setThreadNum(int threadNum) {
        this.threadNum = threadNum;
    }

    public EdgeNetwork getEdgeNetwork() {
        return edgeNetwork;
    }

    public void setEdgeNetwork(EdgeNetwork edgeNetwork) {
        this.edgeNetwork = edgeNetwork;
    }

    public double getIncomingLatency() {
        return incomingLatency;
    }

    public void setIncomingLatency(double incomingLatency) {
        this.incomingLatency = incomingLatency;
    }

    public double getOutgoingLatency() {
        return outgoingLatency;
    }

    public void setOutgoingLatency(double outgoingLatency) {
        this.outgoingLatency = outgoingLatency;
    }
}

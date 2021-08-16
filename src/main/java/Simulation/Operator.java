package Simulation;

import javafx.util.Pair;

import java.util.*;


public class Operator {

    public void setId(int id) {
        this.id = id;
    }

    private String name;
    private int id;
    private final int instructionSize;
    private OperatorType type;

    //selectivity ratio = inRate/outRate
    private final double inRate;
    private final double outRate;
    private final double selectivityRatio;

    //productivity ratio
    private double incomingMessageSize;
    private double outgoingMessageSize;
    private double productivityRatio = 0;


    private EdgeNode assignedEdgeNode;
    private PriorityQueue<Pair<Event, Integer>> savedMessages =
            new PriorityQueue<>((a, b) -> (int) (a.getKey().getReceivingTime() - b.getKey().getReceivingTime()));
    Random random;


    private List<Pair<Double, Double>> runningIntervals = new ArrayList<>();
    private List<Operator> downstreams;
    private List<Operator> upstreams;
    private double incomingDataRate;
    private double outgoingDataRate;

    private Map<Operator, Double> downstreamMessages;
    private Map<Operator, Double> upstreamMessages;


    //Probability rate to each of downstream operator
    private Map<Operator, Double> probabilityRate;

    private Map<Operator, Double> sentMessages = new HashMap<>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Operator(int id, String name, double productivityRatio, double inRate, double outRate, int instructionSize, OperatorType type) {
        this.id = id;
        this.name = name;
        this.productivityRatio = productivityRatio;
        this.inRate = inRate;
        this.outRate = outRate;
        this.selectivityRatio = outRate / inRate;
        this.instructionSize = instructionSize;
        this.type = type;
        this.random = new Random(id + 2);
    }

    public Operator(int id, String name, double incomingMessageSize, double outgoingMessageSize, double inRate, double outRate, int instructionSize, OperatorType type) {
        this.id = id;
        this.name = name;
        this.incomingMessageSize = incomingMessageSize;
        this.outgoingMessageSize = outgoingMessageSize;
        this.inRate = inRate;
        this.outRate = outRate;
        this.selectivityRatio = outRate / inRate;
        this.instructionSize = instructionSize;
        this.type = type;
        this.random = new Random(id + 2);
    }

    public void addProperties(List<Operator> downstreams, List<Operator> upstreams,
                              Map<Operator, Double> probabilityRate) {

        this.downstreams = downstreams;
        this.upstreams = upstreams;
        this.probabilityRate = probabilityRate;

    }

    public List<Integer> receiveEvent(Event event) {
        this.savedMessages.add(new Pair<>(event, event.getNumberOfMessages()));
        if (isThereMessages()) {
            return this.processEvents();
        } else {
            List<Integer> arrayList = new ArrayList<>();
            arrayList.add(-1);
            return arrayList;
        }
//        return isThereMessages() ? this.processEvents() : 0;
    }

    public List<Integer> processEvents() {

        double maxReceivingTime = 0;
        int messages = 0;

        while (messages < inRate) {

            if (savedMessages.peek() != null) {
                messages += savedMessages.peek().getValue();
            }
            assert savedMessages.peek() != null;
            maxReceivingTime = Math.max(maxReceivingTime, savedMessages.peek().getKey().getReceivingTime());
            Pair<Event, Integer> temp = savedMessages.remove();
            if (messages > inRate)
                savedMessages.add(new Pair<>(temp.getKey(),
                        (int) (messages - inRate)));
        }

        double startTime = maxReceivingTime;
        // we should check whether the system is in running state or it is idle
        // if it is in running state we should calculate the idle time which is equal to latest finish time of running interval
        if (runningIntervals.size() > 0) {
            startTime = Math.max(this.runningIntervals.get(runningIntervals.size() - 1).getValue(),
                    maxReceivingTime);
        }
        double finalProcessingTime = startTime + (this.instructionSize / (double) this.assignedEdgeNode.getMIPS());
        this.runningIntervals.add(new Pair<>(startTime, finalProcessingTime));

        List<Integer> results = new ArrayList<>();

        //when it is not the sink
        if (!(this.type == OperatorType.SINK)) {
            //the for is for double operator so we need to send event to both of downstream operators
            specifyEventDestination().forEach(operator -> results.addAll(this.sendEvent(operator,
                    (int) this.outRate,
                    finalProcessingTime)));
            return results;

        } else if ((runningIntervals.size() != 0)) { // when it is sink and it has been run for one time
            results.add(id);
            return results;
        } else {// when it is sink and it was not beeing run
            //we may have two results because of double value
            //if (!results.contains(0)) return results.get(0);
            results.add(-1);
            return results;
        }
    }

    public List<Integer> sendEvent(Operator receiver, int numberOfMessages, double finalProcessingTime) throws NullPointerException {

        Event event;
        if (receiver.getAssignedEdgeNode().equals(this.assignedEdgeNode)) {
            event = new Event(this, receiver, numberOfMessages, this.outgoingMessageSize,
                    finalProcessingTime, finalProcessingTime);
        } else {
            double latency =    this.assignedEdgeNode.getEdgeNetwork().getLatencyPair(this.assignedEdgeNode, receiver.assignedEdgeNode);
            event = new Event(this, receiver, numberOfMessages, this.outgoingMessageSize,
                    finalProcessingTime, finalProcessingTime + latency + this.assignedEdgeNode.getEdgeNetwork().
                    propagationDelay(this.assignedEdgeNode, receiver.getAssignedEdgeNode(),
                            numberOfMessages * outgoingMessageSize));
        }
        Double empty = this.sentMessages.get(receiver);
        if (empty != null)
            this.sentMessages.put(this, this.sentMessages.get(receiver) + numberOfMessages);

        return receiver.receiveEvent(event);
    }


    public boolean isThereMessages() {

        int numberOfReceivedMessage = this.savedMessages.stream().map(Pair::getValue).mapToInt(Integer::intValue).sum();
        return numberOfReceivedMessage >= this.inRate;

    }

    public List<Operator> specifyEventDestination() {

//        Storm.Random random = new Storm.Random();

        var wrapper = new Object() {
            double value;
        };

        HashMap<Operator, Double> newScale = new HashMap<>();
        this.probabilityRate.forEach((key, value) -> {
            wrapper.value += value;
            newScale.put(key, wrapper.value);
        });

        List<Operator> operators = new ArrayList<>();
        double randomValue = this.random.nextDouble();
        for (Map.Entry<Operator, Double> map : newScale.entrySet()) {

            if (map.getValue() >= randomValue) {
                operators.add(map.getKey());

                //if it is not double we need to return one operator so we should break
                //if it is a double it will go for the next operator
                // all the doubles have the probability ratio of 1
                if (map.getValue() != 1)
                    break;
            }
        }
        return operators;
    }

    public OperatorType getType() {
        return type;
    }


    public List<Operator> getUpstreams() {
        return upstreams;
    }


    public void setIncomingAndOutgoingDataRate(double incomingDataRate) {
        this.incomingDataRate = incomingDataRate;
        this.outgoingDataRate = this.incomingDataRate * this.selectivityRatio;
    }

    public double getOutgoingDataRate() {
        return outgoingDataRate;
    }

    public EdgeNode getAssignedEdgeNode() {
        return assignedEdgeNode;
    }

    public void setAssignedEdgeNode(EdgeNode assignedEdgeNode) {
        this.assignedEdgeNode = assignedEdgeNode;
    }

    public int getId() {
        return id;
    }

    public List<Pair<Double, Double>> getRunningIntervals() {
        return runningIntervals;
    }

    public void setRunningIntervals(List<Pair<Double, Double>> runningIntervals) {
        this.runningIntervals = runningIntervals;
    }

    public void setIncomingAndOutgoingMessageSize(double incomingMessageSize) {
        this.incomingMessageSize = incomingMessageSize;
        this.outgoingMessageSize = incomingMessageSize * productivityRatio;
    }

    public double getOutgoingMessageSize() {
        return outgoingMessageSize;
    }

    public void reset() {
        this.setAssignedEdgeNode(null);
        this.setRunningIntervals(new ArrayList<>());
        this.savedMessages = new PriorityQueue<>((a, b) -> (int) (a.getKey().getReceivingTime() - b.getKey().getReceivingTime()));
        this.random = new Random(id);
    }

    public void localRest() {
        this.setRunningIntervals(new ArrayList<>());
        this.savedMessages = new PriorityQueue<>((a, b) -> (int) (a.getKey().getReceivingTime() - b.getKey().getReceivingTime()));
//        this.random = new Storm.Random(id);
    }

    public double getIncomingDataRate() {
        return incomingDataRate;
    }

    public void setIncomingDataRate(double incomingDataRate) {
        this.incomingDataRate = incomingDataRate;
    }

    public int getInstructionSize() {
        return instructionSize;
    }

    public enum OperatorType {
        SOURCE,
        TRANSFORMATION,
        SINK
    }
}


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

    private String messages="";
    private final String messageSeparator =" - ";
    private List<String> sinkMessageSaver = new ArrayList<>();
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
        this.random = new Random(2);
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
        this.random = new Random(2);
    }

    public void addProperties(List<Operator> downstreams, List<Operator> upstreams,
                              Map<Operator, Double> probabilityRate) {

        this.downstreams = downstreams;
        this.upstreams = upstreams;
        this.probabilityRate = probabilityRate;

    }

    public List<Integer> receiveEvent(Event event) {
        this.savedMessages.add(new Pair<>(event, event.getNumberOfMessages()));
        if (this.messages.equals(""))
            this.messages = this.messages + event.getMessage();
        else
            this.messages =   this.messages + this.messageSeparator + event.getMessage();
//        this.messages =  event.getMessage() + " | " + this.messages + " _ " + this.name + " $ ";
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
            //the time when the resource is idle or the time that the message is recived
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
            for (int i = 0; i < outRate ; i++) {
                specifyEventDestination().forEach(operator -> results.addAll(this.sendEvent(operator,
                        (int) this.outRate,
                        finalProcessingTime, this.messages)));
            }
            this.messages ="";
            return results;

        } else { // when it is sink and it has been run for one time
            this.sinkMessageSaver.add(this.messages + " | " + finalProcessingTime);
            results.add(id);
            this.messages ="";
            return results;
        }

//    } else if ((runningIntervals.size() != 0)) { // when it is sink and it has been run for one time
//        this.sinkMessageSaver.add(this.messages + messageSeparator + finalProcessingTime);
//        results.add(id);
//        return results;
//    } else {// when it is sink and it was not beeing run
//        //we may have two results because of double value
//        // we recive a message but it is not enough and we need more
//        //if (!results.contains(0)) return results.get(0);
//        results.add(-1);
//        return results;
//    }
    }

    public List<Integer> sendEvent(Operator receiver, int numberOfMessages, double finalProcessingTime, String messages) throws NullPointerException {

        Event event;
        if (receiver.getAssignedEdgeNode().equals(this.assignedEdgeNode)) {
            event = new Event(this, receiver, messages, numberOfMessages, this.outgoingMessageSize,
                    finalProcessingTime, finalProcessingTime);
        } else {
            double latency = this.assignedEdgeNode.getEdgeNetwork().getLatencyPair(this.assignedEdgeNode, receiver.assignedEdgeNode);
            event = new Event(this, receiver, messages, numberOfMessages, this.outgoingMessageSize,
                    finalProcessingTime, finalProcessingTime + latency + this.assignedEdgeNode.getEdgeNetwork().
                    propagationDelay(this.assignedEdgeNode, receiver.getAssignedEdgeNode(),
                            numberOfMessages * ((receiver.getIncomingMessageSize()))));
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
        //if we have on downstream, or we have a double operator
        if (this.probabilityRate.size() == 1 ||
                probabilityRate.values().stream().mapToDouble(v -> v).sum() == probabilityRate.size()) {
            return new ArrayList<>(probabilityRate.keySet());
        }

        List<Operator> operators = new ArrayList<>();
        HashMap<Operator, Double> newScale = new HashMap<>();

        var wrapper = new Object() {
            double value;
        };
//        int reRangeValue = 0;
//        for (Map.Entry<Operator, Double> map : probabilityRate.entrySet()){
//            if (map.getValue()+ reRangeValue > )
//            map.setValue(map.getValue()+ reRangeValue);
//            reRangeValue++;
//        }
//        int i = 0;
//        if (this.name.equals("v3"))
//            probabilityRate.forEach((key,value) ->System.out.println(key.getName() + "-" + value ));
//        this.probabilityRate =probabi
        this.probabilityRate.forEach((key, value) -> {

            wrapper.value += value;
            newScale.put(key, wrapper.value);
        });
        double randomValue = this.random.nextDouble() * Collections.max(newScale.values());
        for (Map.Entry<Operator, Double> map : newScale.entrySet()) {
//            System.out.println(map.getKey().name);
            if (map.getValue() >= randomValue) {
                operators.add(map.getKey());
//                if (this.name.equals("v3"))
//                    System.out.println(this.name + "-" + randomValue+"-"+map.getKey().name +"-" + map.getValue());
                return operators;
            }
        }
        return null;
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

    public double getIncomingMessageSize(){
        return incomingMessageSize;
    }

    public void reset() {
        this.setAssignedEdgeNode(null);
        this.setRunningIntervals(new ArrayList<>());
        this.savedMessages = new PriorityQueue<>((a, b) -> (int) (a.getKey().getReceivingTime() - b.getKey().getReceivingTime()));
//        this.random = new Random(2);
        this.sinkMessageSaver = new ArrayList<>();
        this.messages="";
    }

    public void localRest() {
        this.setRunningIntervals(new ArrayList<>());
        this.sinkMessageSaver = new ArrayList<>();
        this.savedMessages = new PriorityQueue<>((a, b) -> (int) (a.getKey().getReceivingTime() - b.getKey().getReceivingTime()));
        this.messages="";

//        this.random = new Random(id+2);
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

    public String getMessageSeparator() {
        return messageSeparator;
    }

    public List<String> getSinkMessageSaver() {
        return sinkMessageSaver;
    }

    public void setOutgoingDataRate(double outgoingDataRate) {
        this.outgoingDataRate = outgoingDataRate;
    }

    public enum OperatorType {
        SOURCE,
        TRANSFORMATION,
        SINK
    }

    public OperatorType getOperatorType(){
        return this.type;
    }


}


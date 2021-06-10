package Simulation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class Topology {

    private List<Operator> operators;
    private  Map<Operator,Double> inputDataRate;


    public Topology(List<Operator> operators, Map<Operator,Double> inputDataRate, double incomingMessageSize) {
        this.operators = operators;
        this.inputDataRate = inputDataRate;

        //modifying all the operators in the topology
        updateTopology(inputDataRate, incomingMessageSize);

    }

    public List<Operator> getSource() {

        List<Operator> sources = new ArrayList<>();

        this.getOperators().stream().filter(operator -> operator.getType() == Operator.OperatorType.SOURCE).
                forEach(sources::add);

        return sources;
    }

    public void updateTopology( Map<Operator,Double> inputDataRate, double incomingMessageSize) {

        int i = 0;
        for (Operator operator : this.getSource()) {
            operator.setIncomingAndOutgoingDataRate(inputDataRate.get(operator));
//            operator.setIncomingAndOutgoingMessageSize(incomingMessageSize);
            i++;
        }

        //we have updated the sources in the above lines
        int updateNumber = this.getSource().size();
        int sizeOfTopology = this.getOperators().size();

        do {
            for (Operator o : this.getOperators()) {

                int isReady = 0;
                double IncomingDataRate = 0;

                //if the data is ready from even one upstream operators it will add to the incoming data rate
                for (Operator upsrtream : o.getUpstreams()) {
                    if (upsrtream.getOutgoingDataRate() != 0) {
                        isReady++;
                        IncomingDataRate += upsrtream.getOutgoingDataRate();
                    }
                }

                // if the data rate is added from all upstream operatros it will be ready to run
                //apply the datarate and message size if the operator is ready
                if (o.getUpstreams().size() != 0 && isReady == o.getUpstreams().size()) {
//                    o.setIncomingAndOutgoingMessageSize(o.getUpstreams().get(0).getOutgoingMessageSize());
                    o.setIncomingAndOutgoingDataRate(IncomingDataRate);
                    updateNumber++;
                }

            }
        } while (updateNumber < sizeOfTopology);
    }

    public double finishTime() {

        List<Double> finishTimes = new ArrayList<>();

        this.getSinks().forEach(operator -> {
            int s = operator.getRunningIntervals().size() - 1;
            finishTimes.add(operator.getRunningIntervals().get(s).getValue());
        });
        return finishTimes.stream().mapToDouble(v -> v).max().orElseThrow();

    }

    public List<Operator> getSinks() {
        List<Operator> sinks = new ArrayList<>();

        this.getOperators().stream().filter(operator -> operator.getType() == Operator.OperatorType.SINK).
                forEach(sinks::add);

        return sinks;
    }

    public Operator getOperatorById(int id) {
        return this.operators.stream().filter(operator -> operator.getId() == id).findAny().orElse(null);
    }

    public List<Operator> getOperators() {
        return operators;
    }


    public void resetTopology() {
        this.getOperators().forEach(Operator::reset);
    }

    public void localResetTopology() {
        this.getOperators().forEach(Operator::localRest);
    }
}

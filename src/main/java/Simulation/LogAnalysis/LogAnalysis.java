package Simulation.LogAnalysis;

import Simulation.InfoSave;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public class LogAnalysis {
    //    private static final String[] fileInfo = {"cpu.txt", "inRate.txt", "outRate.txt", "messageBytes.txt"};
    private final String[] LOG_INFO;
    //= {"tuple_inRate.mean_rate", "tuple_outRate.mean_rate", "CPU", "__recv-iconnection"
    //            , "tuple_inRate.count", "tuple_outRate.count"}
    //grep 'tuple_inRate.mean_rate\|tuple_outRate.mean_rate\|CPU\|messageBytes\|tuple_inRate.count\|tuple_outRate.count'  worker.log.metrics.1 > /home/amohtadi/simfiles/metrics.txt
    private final String METRIC_FILE_PATH;// = "/metric-p1000-withoutbuf.txt";
    //    private final String FILE_PATH;// = "/home/amohtadi/simfiles";
    private final String JSON_FILE_PATH;// = "/OperatorDetails.json";

    //Todo: Edit the global and local variables
    //EdgeNode : Machine: resource name for example Logitech4763:6700
    //Task: task id set by apachestorm
    //bolt: bolt vertex id - operator id set by random generate graph
    private String sink;

    Map<String, Integer> inRate = new HashMap<>();
    Map<String, Integer> outRate = new HashMap<>();
    private final Map<String, Double> boltMeanRateMap = new HashMap<>();
    private final Map<String, Integer> totalBoltIncomingTupleMap = new HashMap<>();

    private final Map<String, Integer> totalBoltOutgoingTupleMap = new HashMap<>();
    private final Map<String, List<Integer>> totalIncomingPerMinute = new HashMap<>();
    private final Map<String, List<Integer>> totalOutgoingPerMinute = new HashMap<>();
    private final Map<String, List<Double>> numberOfRunsPerMin = new HashMap<>();
    private final Map<String, Map<String, List<Double>>> metricsMap = new HashMap<>();
    EdgeOperTaskNames edgeOperTaskNames = new EdgeOperTaskNames();

    public LogAnalysis(Properties properties) {

        LOG_INFO = properties.getProperty("log.info").split(",");
        METRIC_FILE_PATH = properties.getProperty("metric.file.path");
        JSON_FILE_PATH = properties.getProperty("json.file.path");
        this.processJson();
        this.process();
        this.setNumberOfRunsPerMin();
    }

    public void processJson() {
        Reader reader = null;
        try {
            reader = Files.newBufferedReader(Path.of(JSON_FILE_PATH));
        } catch (IOException e) {
            e.printStackTrace();
        }

        // convert JSON array to list of users
        List<InfoSave> infoSaves = new Gson().fromJson(reader, new TypeToken<List<InfoSave>>() {
        }.getType());
        infoSaves.forEach(k -> inRate.put(k.getName(), k.getInRate()));
        infoSaves.forEach(k -> outRate.put(k.getName(), k.getOutRate()));
        infoSaves.forEach(k -> {
            if (k.getProbabilityRatio() == null) {
                this.sink = k.getName();
            }
        });
    }

    private void setNumberOfRunsPerMin() {

        for (Map.Entry<String, List<Integer>> map : this.totalOutgoingPerMinute.entrySet()) {
            List<Double> perMin = new ArrayList<>();

            //Todo: should be changes if the sink have different inRate and outRate.
            // Currently, it just works for the sink 1:1

            //if it is sink we need to calculate based on the incoming message
            if (map.getValue().get(0) == 0) {

                //this is the first value so we do not need to mines it with the previous value
                perMin.add(Double.valueOf(this.totalIncomingPerMinute.get(map.getKey()).get(0)) /
                        inRate.get(this.edgeOperTaskNames.getOperatorFromTask(map.getKey())));

                //number of inocming message per min / inRate = number of runs per min
                for (int i = 1; i < map.getValue().size(); i++) {
                    perMin.add((double)
                            (this.totalIncomingPerMinute.get(map.getKey()).get(i) -
                                    this.totalIncomingPerMinute.get(map.getKey()).get(i - 1)) /
                            inRate.get(this.edgeOperTaskNames.getOperatorFromTask(map.getKey())));

                }
            } else { // since it is not sink we can calculate based on the outgoing message
                perMin.add(Double.valueOf(map.getValue().get(0)) /
                        outRate.get(this.edgeOperTaskNames.getOperatorFromTask(map.getKey())));
                for (int i = 1; i < map.getValue().size(); i++) {
                    perMin.add(((double) (map.getValue().get(i) - map.getValue().get(i - 1)) /
                            outRate.get(this.edgeOperTaskNames.getOperatorFromTask(map.getKey()))));
                }
            }
            this.numberOfRunsPerMin.put(map.getKey(), perMin);

        }

    }

    public void process() {

        Map<String, String> edgeNodeTaskMap = new HashMap<>();
        //initialize the metricmap
        for (String meter : LOG_INFO) {
            metricsMap.computeIfAbsent(meter, k -> new HashMap<>());
        }

        //process the lines
        try {

            for (String line : Files.readAllLines(Path.of(METRIC_FILE_PATH))) {
                String[] splitter = line.split("\\s+");
                //setting the pair of nodes and tasks id depolyed to the nodes
                if (!splitter[5].equals("-1:__system")) {
                    edgeNodeTaskMap.put(splitter[4], splitter[5]);
                }
                Map<String, List<Double>> deviceValueMap = metricsMap.get(splitter[6]);
                String device = splitter[4];


                if (splitter[6].equals(LOG_INFO[3])) {   //MessageSize
                    String messageBytes = line.substring(line.indexOf("messageBytes=") + 14);
                    String[] messageBytesSplit = messageBytes.split("\\s+");

                    for (String pair : messageBytesSplit) {
                        String[] taskIndex = pair.replaceAll("[^0-9]+", " ").split(" ");
                        if (taskIndex.length != 0) {

                            String id;
                            if (taskIndex[0].equals("")) {
                                id = "-" + taskIndex[1] + "-" + taskIndex[2];
                                deviceValueMap.computeIfAbsent(id, k -> new ArrayList<>()).
                                        add(Double.valueOf(taskIndex[3]));

                            } else {
                                id = taskIndex[0] + "-" + taskIndex[1];
                                deviceValueMap.computeIfAbsent(id, k -> new ArrayList<>()).
                                        add(Double.valueOf(taskIndex[2]));

                            }
                        }
                    }

                } else if (splitter[6].equals(LOG_INFO[2])) {    //Cpu
                    String value = splitter[7].replaceFirst(".*?(\\d+).*", "$1");
                    deviceValueMap.computeIfAbsent(device, k -> new ArrayList<>()).add(Double.valueOf(value));

                } else if (splitter[6].equals(LOG_INFO[0])) { // message per second (multiplied by 1000000)
                    boltMeanRateMap.put(splitter[5].split(":")[1], Double.parseDouble(splitter[7]) * 1000000);

                } else if (splitter[6].equals(LOG_INFO[4])) { //inRate
                    String bolt = splitter[5].split(":")[0];
                    int value = Integer.parseInt(splitter[7]);
                    this.totalIncomingPerMinute.computeIfAbsent(bolt, k -> new ArrayList<>()).add(value);
                    //simple: since always we have higher value we can simply put every single value
                    this.totalBoltIncomingTupleMap.put(bolt, value);

                } else if (splitter[6].equals(LOG_INFO[5])) {//out
                    String bolt = splitter[5].split(":")[0];
                    int value = Integer.parseInt(splitter[7]);
                    this.totalOutgoingPerMinute.computeIfAbsent(bolt, k -> new ArrayList<>()).add(value);
                    //simple: since always we have higher value we can simply put every single value
                    this.totalBoltOutgoingTupleMap.put(bolt, Integer.parseInt(splitter[7]));

                } else {    //others
                    String value = splitter[7];
                    deviceValueMap.computeIfAbsent(device, k -> new ArrayList<>()).add(Double.valueOf(value));

                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        setNames(edgeNodeTaskMap);
    }

    public void setNames(Map<String, String> edgeNodeOper) {

        for (Map.Entry<String, String> map : edgeNodeOper.entrySet()) {
            String edgeName = map.getKey();
            String taskName = map.getValue().split(":")[0];
            String operatorName = map.getValue().split(":")[1];
            this.edgeOperTaskNames.add(edgeName, taskName, operatorName);
        }
    }

    public Map<String, Double> instructionSize(double testBedInstSize) {

        Map<String, Double> instructionSize = new HashMap<>();
        for (Map.Entry<String, List<Double>> cpu : this.metricsMap.get("CPU").entrySet()) {
            List<Double> averageCPU = new ArrayList<>();

            //we calculate how many times an operator was run in each minutes and then we divided the CPU value by the
            // number of runs to get the cpu usage per run
            if (this.edgeOperTaskNames.getTaskFromEdge(cpu.getKey()) != null) {
                List<Double> numberOfRuns = this.numberOfRunsPerMin.get(this.edgeOperTaskNames.
                        getTaskFromEdge(cpu.getKey()));

                //the first cpu is huge, so we check from the second
                for (int i = 3; i < Math.min(numberOfRuns.size(),cpu.getValue().size()); i++) {
                    //if the operator have been run for one time
                    if (numberOfRuns.get(i) != 0) {
                        //Todo: outRate is set one for the sink. Because we used inRate for the sink. so we do not need
                        // to divide this by the outRate.
                        //remove 10000
                        //averageCPU.add((cpu.getValue().get(i) +10000) / numberOfRuns.get(i));
                        averageCPU.add((cpu.getValue().get(i)-3000) / numberOfRuns.get(i));
                    }
                }

                //if it has been run
                if (averageCPU.size()!=0)
                // in second (divide by 1000 = ms to second)
                    instructionSize.put(this.edgeOperTaskNames.getOperatorFromEdge(cpu.getKey()), averageCPU.stream()
                        .mapToDouble(t -> t).average().orElseThrow() * testBedInstSize / 1000);
                else instructionSize.put(this.edgeOperTaskNames.getOperatorFromEdge(cpu.getKey()),0.0); // if the operator never runs
            }
        }
        return instructionSize;
    }


    public Map<String, Double> productivityRatio() {
        Map<String, List<Double>> temp = new HashMap<>();
        Map<String, Double> messageSizeAverage = new HashMap<>();


        List<String> taskId = this.edgeOperTaskNames.getTasksName();

        this.metricsMap.get(LOG_INFO[3]).forEach((k, v) -> messageSizeAverage.put(k, v.stream().mapToDouble(t -> t).
                sum()));

        Map<String, Double> result = new HashMap<>();

        for (Map.Entry<String, Double> sourcePair : messageSizeAverage.entrySet()) {
            String source1 = sourcePair.getKey().split("-")[1];
            String target1 = sourcePair.getKey().split("-")[0];
            double inMessageSize = sourcePair.getValue();

            if (taskId.contains(source1) && taskId.contains(target1)) {
                for (Map.Entry<String, Double> targetPair : messageSizeAverage.entrySet()) {
                    String source2 = targetPair.getKey().split("-")[0];
                    String target2 = targetPair.getKey().split("-")[1];
                    double outMessageSize = targetPair.getValue();

                    if (source1.equals(source2) && taskId.contains(target2)) {
                        temp.computeIfAbsent(source2, k -> new ArrayList<>()).add(inMessageSize / outMessageSize);
                    }
                }
            }
        }
        temp.forEach((k, v) -> result.put(this.edgeOperTaskNames.getOperatorFromTask(k), v.stream().mapToDouble(t -> t).
                average().orElseThrow()));

        return result;
    }

    //we calculate the sum of all the message bytes transfered among rthe devices and then we divide it by the total number of messages beein g transfered
    public List<Map<String, Double>> calculateMessageSizes() {
        Map<String, List<Double>> temp = new HashMap<>();
        Map<String, Double> messageSizeSum = new HashMap<>();
        Map<String, Double> inMessageSize = new HashMap<>();
        Map<String, Double> outMessageSize = new HashMap<>();

        Map<String, Double> inMessageSizeUpdated = new HashMap<>();
        Map<String, Double> outMessageSizeUpdated = new HashMap<>();

        List<String> taskId = this.edgeOperTaskNames.getTasksName();
//Map <String,Double> sourceAndSink =
         this.metricsMap.get(LOG_INFO[3]).entrySet().stream().filter(map ->
                taskId.contains(map.getKey().split("-")[0])
                        && taskId.contains(map.getKey().split("-")[1])).
                collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)).
                forEach((k, v) -> messageSizeSum.put(k, v.stream().mapToDouble(t -> t).sum()));
        messageSizeSum.forEach((k, v) ->
        {
            inMessageSize.put(k.split("-")[1],
                    inMessageSize.getOrDefault(k.split("-")[1], 0.0) + v);
            outMessageSize.put(k.split("-")[0],
                    outMessageSize.getOrDefault(k.split("-")[0], 0.0) + v);
        });

        //update the values
        inMessageSize.forEach((k, v) -> inMessageSizeUpdated.
                put(this.edgeOperTaskNames.getOperatorFromTask(k), v / totalBoltIncomingTupleMap.get(k)));
        outMessageSize.forEach((k, v) -> outMessageSizeUpdated.
                put(this.edgeOperTaskNames.getOperatorFromTask(k), v / totalBoltOutgoingTupleMap.get(k)));

        List<Map<String, Double>> result = new ArrayList<>();
        //check if there is an unused operator that was never run
        if (this.edgeOperTaskNames.getEdgeTaskBoltNames().size()!=inMessageSize.size()){
            for (EdgeOperTaskNames.EdgeOperTaskName edgeOperTaskName : this.edgeOperTaskNames.getEdgeTaskBoltNames()){
                if (!inMessageSizeUpdated.containsKey(edgeOperTaskName.operatorName)
                        && !outMessageSizeUpdated.containsKey(edgeOperTaskName.operatorName)) {
                    inMessageSizeUpdated.put(edgeOperTaskName.getOperatorName(), 0.0);
                    outMessageSizeUpdated.put(edgeOperTaskName.getOperatorName(), 0.0);
                }
            }
        }
        result.add(inMessageSizeUpdated);
        result.add(outMessageSizeUpdated);

        return result;


    }

    public Map<String, Double> calculateInputRate() {
//         return boltMeanRateMap.entrySet().stream().filter(map -> spouts.contains(map.getKey())).
//               collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
//        List<String> spoutTasks = new ArrayList<>();
//        spouts.forEach(v -> spoutTasks.add(edgeOperTaskNames.getTaskFromOperator(v)));
        Map<String, Double> inputRates = new HashMap<>();
        //return spoutTasks.forEach(v -> inputRates.put(totalIncomingPerMinute.get(v).get(totalIncomingPerMinute.get(v).size())) );
        for (Map.Entry<String, Integer> totalBoltIncoming : totalBoltIncomingTupleMap.entrySet()) {
            String taskName = totalBoltIncoming.getKey();
            int totalIncomingMessage = totalBoltIncomingTupleMap.get(taskName);

            List<Integer> incomingPerMin = totalIncomingPerMinute.get(taskName);

            if (incomingPerMin.contains(0)) {
                incomingPerMin.remove(Integer.valueOf(0));
            }
            inputRates.put(edgeOperTaskNames.getOperatorFromTask(taskName), (double) totalIncomingMessage / (incomingPerMin.size() * 60));

        }
        return inputRates;
    }

    public Map<String, Double> calculateOutputRate() {
        Map<String, Double> outputRates = new HashMap<>();
        for (Map.Entry<String, Integer> totalBoltOutgoing : totalBoltOutgoingTupleMap.entrySet()) {
            String taskName = totalBoltOutgoing.getKey();
            int totalOutgoingMessage = totalBoltOutgoingTupleMap.get(taskName);
            List<Integer> outgoingPerMin = totalOutgoingPerMinute.get(taskName);

            if (outgoingPerMin.contains(0)) {
                outgoingPerMin.remove(Integer.valueOf(0));
            }

            outputRates.put(edgeOperTaskNames.getOperatorFromTask(taskName), (double) totalOutgoingMessage / (outgoingPerMin.size() * 60));
        }
        return outputRates;

    }

    public ArrayList<Integer> getDeployment(Map<String, Integer> edgeNodesOrder){
        ArrayList<Integer> arrayList = new ArrayList<>();
//        edgeOperTaskNames.getEdgeTaskBoltNames().sort(Comparator.comparing(EdgeOperTaskNames.EdgeOperTaskName::getOperatorName));
        Collections.sort(edgeOperTaskNames.getEdgeTaskBoltNames());
        for (EdgeOperTaskNames.EdgeOperTaskName e : edgeOperTaskNames.getEdgeTaskBoltNames()){
            arrayList.add(edgeNodesOrder.get(e.getEdgeName().split(":")[0]));
        }
        return arrayList;
    }

// json -> probability ratio
// log -> instructionSize
// dot file -> dag structure


}




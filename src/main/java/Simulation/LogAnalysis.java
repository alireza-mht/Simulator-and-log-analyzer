package Simulation;

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
    private final String[] LOG_INFO ;
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
    private final Map<String, String> edgeNodeBolt = new HashMap<>();
    private final Map<String, String> edgeNodeTaskMap = new HashMap<>();
    Map<String, Integer> inRate = new HashMap<>();
    Map<String, Integer> outRate = new HashMap<>();
    private final Map<String, Double> boltMeanRateMap = new HashMap<>();
    private final Map<String, Integer> totalBoltIncomingTupleMap = new HashMap<>();
    private final Map<String, Integer> boltLogInfoUpTimeMap = new HashMap<>();
    private final Map<String, Integer> totalBoltOutgoingTupleMap = new HashMap<>();
    private final Map<String, List<Integer>> totalIncomingPerMinute = new HashMap<>();
    private final Map<String, List<Integer>> totalOutgoingPerMinute = new HashMap<>();
    private final Map<String, List<Double>> numberOfRunsPerMin = new HashMap<>();
    //    private final Set<String> edgeNodesIdSet = new HashSet<>();
    private final Map<String, Map<String, List<Double>>> metricsMap = new HashMap<>();
    private final Map<String, String> taskBoltIdMap = new HashMap<>();

    public LogAnalysis( Properties properties) {

        LOG_INFO =properties.getProperty("log.info").split(",");
        METRIC_FILE_PATH =  properties.getProperty("metric.file.path");
        JSON_FILE_PATH=  properties.getProperty("json.file.path");
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
    }

    private void setNumberOfRunsPerMin() {

        for (Map.Entry<String, List<Integer>> map : this.totalOutgoingPerMinute.entrySet()) {
            List<Double> perMin = new ArrayList<>();

            //Todo: should be changes if the sink have different inRate and outRate.
            // Currently, it just works for the sink 1:1
            if (map.getValue().get(0) == 1) {
                perMin.add(Double.valueOf(this.totalIncomingPerMinute.get(map.getKey()).get(0))/
                        inRate.get(taskBoltIdMap.get(map.getKey())));
                for (int i = 1; i < map.getValue().size(); i++) {
                    perMin.add((double)
                            (this.totalIncomingPerMinute.get(map.getKey()).get(i) -
                                    this.totalIncomingPerMinute.get(map.getKey()).get(i - 1))/
                            inRate.get(taskBoltIdMap.get(map.getKey())));
                }


            } else {
                perMin.add(Double.valueOf(map.getValue().get(0))/ outRate.get(taskBoltIdMap.get(map.getKey())));
                for (int i = 1; i < map.getValue().size(); i++) {
                    perMin.add(((double) (map.getValue().get(i) - map.getValue().get(i - 1)) / outRate.get(taskBoltIdMap.get(map.getKey()))));
                }
            }
            this.numberOfRunsPerMin.put(map.getKey(), perMin);

        }

    }

    public void process() {

        //initialize the metricmap
        for (String meter : LOG_INFO) {
            metricsMap.computeIfAbsent(meter, k -> new HashMap<>());
        }

        //process the lines
        try {
            int sumOfRuns = 0;
            for (String line : Files.readAllLines(Path.of(METRIC_FILE_PATH))) {

                String[] splitter = line.split("\\s+");

                //setting the pair of nodes and tasks id depolyed to the nodes
                if (!splitter[5].equals("-1:__system")) {
                    edgeNodeTaskMap.put(splitter[4], splitter[5]);
                    edgeNodeBolt.put(splitter[4], splitter[5].split(":")[1]);
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
                                deviceValueMap.computeIfAbsent(id, k -> new ArrayList<>()).add(Double.valueOf(taskIndex[3]));
                            } else {
                                id = taskIndex[0] + "-" + taskIndex[1];
                                deviceValueMap.computeIfAbsent(id, k -> new ArrayList<>()).add(Double.valueOf(taskIndex[2]));
                            }
                        }
                    }

                } else if (splitter[6].equals(LOG_INFO[2])) {    //Cpu

                    String value = splitter[7].replaceFirst(".*?(\\d+).*", "$1");
                    deviceValueMap.computeIfAbsent(device, k -> new ArrayList<>()).add(Double.valueOf(value));
                } else if (splitter[6].equals(LOG_INFO[0])) {
                    boltMeanRateMap.put(splitter[5].split(":")[1], Double.parseDouble(splitter[7])*1000000);
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

//        edgeNodeTaskMap.forEach((k, v) -> edgeNodesIdSet.remove(k));
        this.edgeNodeTaskMap.forEach((k, v) -> taskBoltIdMap.put(v.split(":")[0], v.split(":")[1]));

    }

    public Map<String, Double> instructionSize(double testBedInstSize) {


        Map<String, Double> instructionSize = new HashMap<>();
        for (Map.Entry<String, List<Double>> cpu : this.metricsMap.get("CPU").entrySet()) {
            List<Double> averageCPU = new ArrayList<>();
            if (this.edgeNodeTaskMap.containsKey(cpu.getKey())) {
                List<Double> numberOfRuns = this.numberOfRunsPerMin.get(this.edgeNodeTaskMap.get(cpu.getKey()).split(":")[0]);


                for (int i = 0; i < numberOfRuns.size(); i++) {
                    if (numberOfRuns.get(i) != 0) {
                        //Todo: outRate is set one for the sink. Because we used inRate for the sink. so we do not need
                        // to divide this by the outRate.
                        averageCPU.add(cpu.getValue().get(i) / numberOfRuns.get(i));

                    }
                }
                instructionSize.put(this.edgeNodeTaskMap.get(cpu.getKey()).split(":")[1], averageCPU.stream().mapToDouble(t -> t)
                        .average().orElseThrow()*testBedInstSize/1000);
            }
        }
        return instructionSize;
    }

    public Map<String, Double> productivityRatio() {
        Map<String, List<Double>> temp = new HashMap<>();
        Map<String, Double> messageSizeAverage = new HashMap<>();


        Set<String> taskId = taskBoltIdMap.keySet();

        this.metricsMap.get(LOG_INFO[3]).forEach((k, v) -> messageSizeAverage.put(k, v.stream().mapToDouble(t -> t).sum()));

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
        temp.forEach((k, v) -> result.put(taskBoltIdMap.get(k), v.stream().mapToDouble(t -> t).average().orElseThrow()));

        return result;
    }

    public List<Map<String, Double>> messageSizes() {
        Map<String, List<Double>> temp = new HashMap<>();
        Map<String, Double> messageSizeSum = new HashMap<>();
        Map<String, Double> inMessageSize = new HashMap<>();
        Map<String, Double> outMessageSize = new HashMap<>();

        Map<String, Double> inMessageSizeUpdated = new HashMap<>();
        Map<String, Double> outMessageSizeUpdated = new HashMap<>();

        Set<String> taskId = taskBoltIdMap.keySet();

        this.metricsMap.get(LOG_INFO[3]).entrySet().stream().filter(map -> taskId.contains(map.getKey().split("-")[0])
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
        inMessageSize.forEach((k, v) -> inMessageSizeUpdated.put(taskBoltIdMap.get(k), v / totalBoltIncomingTupleMap.get(k)));
        outMessageSize.forEach((k, v) -> outMessageSizeUpdated.put(taskBoltIdMap.get(k), v / totalBoltOutgoingTupleMap.get(k)));

        List<Map<String, Double>> result = new ArrayList<>();
        result.add(inMessageSizeUpdated);
        result.add(outMessageSizeUpdated);
        return result;


    }

    public Map<String, Double> inputRate(List<String> spouts) {

        return boltMeanRateMap.entrySet().stream().filter(map -> spouts.contains(map.getKey())).
                collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    }

// json -> probability ratio
// log -> instructionSize
// dot file -> dag structure


}




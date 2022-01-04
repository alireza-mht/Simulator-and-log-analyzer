package Simulation;

import Simulation.GWO.*;
import Simulation.LogAnalysis.LogAnalysis;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import javafx.util.Pair;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.nio.dot.DOTImporter;
import org.jgrapht.traverse.TopologicalOrderIterator;

import java.io.*;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class Problem {
    private EdgeNetwork edgeNetwork;
    private Topology topology;
    private Map<Operator, List<EdgeNode>> operatorsAvailableEdgeNodes;
    Map<String, Integer> edgeNodesOrder;
    List<Integer> serverDeployment;
    Properties mainProperties;
    private final int EDGE_SIZE;
    private final int OPERATOR_SIZE;
    private final Map<Operator, Double> INPUT_DATA_RATE = new HashMap<>();
    private final int TIMES_OF_RUNS_PER_ASSIGNMENT;
    private final int ITERATION_NUMBER;
    private final int POPULATION_NUMBER;
    private final double MESSAGE_SIZE = 2;
    private final String DOT_FILE_PATH;
    private final String JSON_FILE_PATH;
    private final String RESULT_FILE_PATH;
    private final String BANDWIDTH_PATH;
    private final String PI_LIST_PATH;
    private final String THREAD_LIST_PATH;
    private int TEST_BED_INSTRUCTION_SIZE;

    public Problem(String propertyPath) throws IOException {

        this.mainProperties = new Properties();

        FileInputStream file;

        //the base folder is ./, the root of the main.properties file

        //load the file handle for main.properties
        file = new FileInputStream(propertyPath);

        //load all the properties from this file
        mainProperties.load(file);

        //we have loaded the properties, so close the file handle
        file.close();

        EDGE_SIZE = Integer.parseInt(mainProperties.getProperty("edge.size")); //30
        OPERATOR_SIZE = Integer.parseInt(mainProperties.getProperty("operator.size")); //9
        TIMES_OF_RUNS_PER_ASSIGNMENT = Integer.parseInt(mainProperties.getProperty("runs.per.assignment")); //1
        ITERATION_NUMBER = Integer.parseInt(mainProperties.getProperty("iteration.number"));//100
        POPULATION_NUMBER = Integer.parseInt(mainProperties.getProperty("population.number")); //100
        JSON_FILE_PATH = mainProperties.getProperty("json.file.path");
        DOT_FILE_PATH = mainProperties.getProperty("dot.file.path");
        RESULT_FILE_PATH = mainProperties.getProperty("result.file.path"); //
        TEST_BED_INSTRUCTION_SIZE = Integer.parseInt(mainProperties.getProperty("testbed.instruction.size")); //2451
        BANDWIDTH_PATH = mainProperties.getProperty("bandwidth.path");
        PI_LIST_PATH = mainProperties.getProperty("pi.list.path");
        THREAD_LIST_PATH = mainProperties.getProperty("thread.list.path");


        this.edgeNetwork = this.generateEdgeNetwork();
        this.topology = this.generateTopology();
        this.operatorsAvailableEdgeNodes = new HashMap<>();
        this.calculateAvailableEdgeNodes();
    }

    public void solve() {


        int D = topology.getOperators().size();

        Function function = new Function(this.topology.getOperators().size()) {
            @Override
            public double eval(List<Integer> args) throws DimensionsUnmatchedException {

                args = serverDeployment;
                //check if the results contains one resource
                int test = isValid(args);
                if (test == -1)
                    return Double.POSITIVE_INFINITY;

                Topology finalTopology = topology;
                for (int index = 0; index < finalTopology.getOperators().size(); index++) {
                    Operator o = finalTopology.getOperators().get(index);
                    EdgeNode e = operatorsAvailableEdgeNodes.get(o).get(args.get(index) - 1);
                    o.setAssignedEdgeNode(e);
                    e.setPlacedOperator(o);
                }
//                topology.getOperators().forEach(operator -> {
//
//                    int edgeNodeIndex = args.get(finalTopology.getOperators().indexOf(operator));
//
//                    operator.setAssignedEdgeNode(operatorsAvailableEdgeNodes.get(operator).get(edgeNodeIndex - 1));
//                    operatorsAvailableEdgeNodes.get(operator).get(edgeNodeIndex - 1).setPlacedOperator(operator);
//                });

                List<Double> finishTime = new ArrayList<>();
                List<Integer> running = new ArrayList<>();
//                for (int)
//                running.stream().forEach(a -> {
//                    pushData(topology);
//                    finishTime.add(topology.finishTime());
////                    ConcurrentPushData concurrentPushData = new ConcurrentPushData();
////                    finishTime.add(concurrentPushData.pushData(topology, INPUT_DATA_RATE));
//                    topology.localResetTopology();
//                    edgeNetwork.localRestEdgeNetwork();
//                });
                for (int i = 0; i < TIMES_OF_RUNS_PER_ASSIGNMENT; i++) {
//                ConcurrentPushData concurrentPushData = new ConcurrentPushData();
//                finishTime.add(concurrentPushData.pushData(topology, INPUT_DATA_RATE));
                    pushData(topology);
                    finishTime.add(topology.finishTime());
                    topology.localResetTopology();
                    edgeNetwork.localRestEdgeNetwork();
                }
                topology.resetTopology();
                edgeNetwork.restEdgeNetwork();
                return finishTime.stream().mapToDouble(v -> v).average().orElseThrow();
            }
        };
        List<Integer> low = new ArrayList<>(Collections.nCopies(topology.getOperators().size(), 1));
//        List<Integer> upp = new ArrayList<>(Collections.nCopies(topology.getOperators().size(), EDGE_SIZE));
        List<Integer> upp = new ArrayList<>();

        for (Operator operator : topology.getOperators()) {
            upp.add(operatorsAvailableEdgeNodes.get(operator).size());
        }

        WolfPackParameters params = new WolfPackParameters(D);
        params.setPackParameters(POPULATION_NUMBER, ITERATION_NUMBER);
        //we set the maximum number of edge node that could be selected by our geo
        //So the domain of selection would be limited for each operator
        if (!low.isEmpty() && !upp.isEmpty()) {
            params.setLimits(low, upp);
        }

        WolfPack pack = new WolfPack();

//        System.out.println("Minimum:");
        WolfPackSolution solution = pack.findMinimum(function, params);

//        while (solution == null) {
//            try {
//                solution = pack.findMinimum(function, params);
//            } catch (Exception e) {
//                System.out.println(e);
//            }
//        }
        //first element in solution integer is the first element in operator avalible edge node
        // the vlaue of solution integer is assined based on the order of value in operatorAvalibleEdgeNode

        List<Integer> solutionsInteger;
        solutionsInteger = solution.bestSolutionInteger();
        FileWriter fileWriter;
        try {
            fileWriter = new FileWriter(this.RESULT_FILE_PATH);
            int q = 0;
            for (Operator operator : topology.getOperators()) {
                fileWriter.write(operatorsAvailableEdgeNodes.get(operator).
                        get(solutionsInteger.get(q) - 1).getName() + " " + "v" + operator.getId() + "\n");
                q++;
            }
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


        System.out.println(solution);


//        int q = 0;
//        for (Operator operator : topology.getOperators()) {
//            System.out.print(operatorsAvailableEdgeNodes.get(operator).get(solutionsInteger.get(q) - 1).getId() + ", ");
//            q++;
//
//        }

    }

    public void pushData(Topology topology) {
//        BigDecimal updateTopologyTime;
//        if (INPUT_DATA_RATE.size() == 1) {
//            updateTopologyTime = BigDecimal.valueOf(1 / INPUT_DATA_RATE.entrySet().stream().
//                    max(Map.Entry.comparingByValue()).get().getValue());
//        } else {
//            ArrayList<Double> dataRateValues = new ArrayList<Double>(this.INPUT_DATA_RATE.values());
//            updateTopologyTime = gcd(BigDecimal.valueOf(dataRateValues.get(0)), BigDecimal.valueOf(dataRateValues.get(1)));
//            for (int i = 2; i < dataRateValues.size(); i++) {
//                updateTopologyTime = gcd(updateTopologyTime, BigDecimal.valueOf(dataRateValues.get(i)));
//            }
//
//        }

        //the id list of finished operator, it contains unique values. we continue running the simulation until
        // all the sinks to receive the message.
        final List<Integer> finish = new ArrayList<>();
//        BigDecimal time = new BigDecimal(0);
        Map<Integer,Double> sendingTimes = new HashMap<>();
        Map<Operator, Double> messageSendTime = new HashMap<>();
        for (Operator operator : topology.getSource()) {
            messageSendTime.put(operator, 0.0);
        }
        Map<Operator, Double> tempMessageSendTime;
        int numberOfMessage = 0;
        do {
            tempMessageSendTime = messageSendTime;
            for (Map.Entry<Operator, Double> map : messageSendTime.entrySet()) {
                tempMessageSendTime.put(map.getKey(), map.getValue() + ((1 / INPUT_DATA_RATE.get(map.getKey()))));
            }

            Operator operator = tempMessageSendTime.entrySet().stream()
                    .min((e1, e2) -> e1.getValue() <= e2.getValue() ? -1 : 1).get().getKey();
//            Operator operator = get
//            for (Operator operator : topology.getSource()) {
            double time = messageSendTime.get(operator);
//                double value = time.doubleValue() % (1 / INPUT_DATA_RATE.get(operator));
//                if ((1 / INPUT_DATA_RATE.get(operator) - value) < 1.469446951953614E-17) {

            Event event = new Event(null, operator, "id: " + numberOfMessage , 1, 0, time,
                    time);
            sendingTimes.put(numberOfMessage,time);

            List<Integer> temp = operator.receiveEvent(event);
            //we recive id or -1, if it is id it means that the mentioned operator is finished
            temp = temp.stream().filter(o -> o != -1).collect(Collectors.toList());

            if (temp.size() != 0)
                temp.stream().filter(o -> topology.getOperatorById(o).getType() == Operator.OperatorType.SINK &&
                        !finish.contains(o)).forEach(finish::add);
            time = messageSendTime.get(operator) + ((1 / INPUT_DATA_RATE.get(operator)));
            messageSendTime.put(operator, time);

            numberOfMessage++;

//                }
//            }
//            time = time.add(updateTopologyBig);
//
//            time += 1;
//            int i = 0;
//            for (Operator operator : topology.getSource()) {
//                if (INPUT_DATA_RATE.get(operator) <= 1) {
//                    sum += INPUT_DATA_RATE.get(operator);
//                    numberOfMessage++;
//                    Event event = new Event(null, operator, (int) Math.floor(sum), 0, time,
//                            time);
//                    if (sum > 1) {
//                        sum -= 1;
//                        numberOfMessage = 0;
//                    }
//                    List<Integer> temp = operator.receiveEvent(event);
//                    temp = temp.stream().filter(o -> o != -1).collect(Collectors.toList());
//
//                    if (temp.size() != 0)
//                        temp.stream().filter(o -> topology.getOperatorById(o).getType() == Operator.OperatorType.SINK &&
//                                !finish.contains(o)).forEach(finish::add);
//                }else if (INPUT_DATA_RATE.get(operator)>1){
//                    int splittedTime = 0;
//                    double timePerMessage = 1/INPUT_DATA_RATE.get(operator);
//                    sum +=timePerMessage;
//                    numberOfMessage++;
//                    Event event = new Event(null, operator, 1, 0, sum,
//                            time);
//                }
//                i++;
//
//            }

//      } while (time.doubleValue() < 2);
        } while (!(finish.size() == topology.getSinks().size()));
        topology.setSendTimes(sendingTimes);
//        System.out.println("hi");
    }

    //second constraint on the number of threads on teach edge node
    public int isValid(List<Integer> args) {
        List<Integer> edgeNodesIDs = new ArrayList<>();
        // args give us the id of edge node
        for (Operator operator : topology.getOperators()) {

            int EdgeNodeIndex = args.get(topology.getOperators().indexOf(operator));
            if (operatorsAvailableEdgeNodes.get(operator).get(EdgeNodeIndex - 1) == null)
                return -1;
            edgeNodesIDs.add(operatorsAvailableEdgeNodes.get(operator).get(EdgeNodeIndex - 1).getId());
        }

        Map<Integer, Integer> repetitive = new HashMap<>();
        for (Integer edgeNodeId : edgeNodesIDs) {

            repetitive.putIfAbsent(edgeNodeId, 0);
            repetitive.put(edgeNodeId, repetitive.get(edgeNodeId) + 1);
        }
        for (EdgeNode edgeNode : this.edgeNetwork.getEdgeNodes()) {
            if (repetitive.containsKey(edgeNode.getId())) {
                if (repetitive.get(edgeNode.getId()) > edgeNode.getThreadNum())
                    return -1;
            }
        }
//        //multiple operators on one edge node
//        Set<Integer> duplicate = new HashSet<>(edgeNodesIDs);
//        if (duplicate.size() != args.size())
//            return -1;

        return 0;
    }


    public Topology generateTopology() {
        //jgrapht;
        List<Operator> operators = generateOperators();
        return new Topology(operators, INPUT_DATA_RATE, MESSAGE_SIZE);
    }

    public EdgeNetwork generateEdgeNetwork() {

        List<EdgeNode> edgeNodes = generateEdgeNodes(EDGE_SIZE);
//        return new EdgeNetwork(edgeNodes, generateBandwidth(edgeNodes), generateLatency(edgeNodes));
        return new EdgeNetwork(edgeNodes);
    }

    public List<EdgeNode> generateEdgeNodes(int size) {
        Map<Pair<EdgeNode, EdgeNode>, Double> latency = new HashMap<>();

        List<EdgeNode> edgeNodes = new ArrayList<>();
        String bandwidthPath = BANDWIDTH_PATH;
        String hostPath = PI_LIST_PATH;
        String threadPath = THREAD_LIST_PATH;
        this.edgeNodesOrder = new HashMap<>();
        try {
            int i = 0;
            while (i < size) {
                String line = Files.readAllLines(Path.of("./").resolve(bandwidthPath)).get(i);
                String name = Files.readAllLines(Path.of("./").resolve(hostPath)).get(i);

                //in byte per second
                double inBand = Double.parseDouble(line.split(" ")[0]) * 1000000 / 8;
                //latency in second
                double inLatency = Double.parseDouble(line.split(" ")[1]) / 1000;
                double outBand = Double.parseDouble(line.split(" ")[2]) * 1000000 / 8;
                double outLatency = Double.parseDouble(line.split(" ")[3]) / 1000;

                int thread = Integer.parseInt(Files.readAllLines(Path.of("./").resolve(threadPath)).get(i));

                EdgeNode edgeNode = new EdgeNode(i, name, thread, TEST_BED_INSTRUCTION_SIZE, inBand, outBand, inLatency, outLatency);
                edgeNodes.add(edgeNode);

                i += 1;
                edgeNodesOrder.put(name, i);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
//        // Connecting all the edge nodes
        for (EdgeNode i : edgeNodes) {
            for (EdgeNode j : edgeNodes) {
                if (i != j) {
                    i.addNeighbour(j);
                }
            }
        }

        return edgeNodes;
    }

    public static Map<Pair<EdgeNode, EdgeNode>, Double> generateBandwidth(List<EdgeNode> edgeNodes) {
        Map<Pair<EdgeNode, EdgeNode>, Double> bandwidth = new HashMap<>();
        for (EdgeNode i : edgeNodes) {
            for (EdgeNode j : edgeNodes) {
                if (i != j && i.getNeighbours().contains(j)) {

                    double minBandwidth = (Math.min(i.getOutgoingBandwidth(), j.getIncomingBandwidth()));
                    bandwidth.put(new Pair<>(i, j), minBandwidth);

                }
            }
        }
        return bandwidth;
    }

    public static Map<Pair<EdgeNode, EdgeNode>, Double> generateLatency(List<EdgeNode> edgeNodes) {
        Map<Pair<EdgeNode, EdgeNode>, Double> latency = new HashMap<>();
        for (EdgeNode i : edgeNodes) {
            for (EdgeNode j : edgeNodes) {
                if (i != j && i.getNeighbours().contains(j)) {

                    latency.put(new Pair<>(i, j), i.getOutgoingLatency() + j.getIncomingLatency());

                }
            }
        }
        return latency;
    }

    public List<Operator> generateOperators() {

        //reading the instruction size
        LogAnalysis logAnalysis = new LogAnalysis(this.mainProperties);
        Map<String, Double> instructionsSize = logAnalysis.instructionSize(TEST_BED_INSTRUCTION_SIZE);
        List<Map<String, Double>> messageSizes = logAnalysis.messageSizes();
        this.serverDeployment = logAnalysis.getDeployment(this.edgeNodesOrder);


        //Reading the probability ratio from json file
        // create a reader
        List<InfoSave> infoSaves = null;
        Reader reader;
        try {
            reader = Files.newBufferedReader(Path.of(JSON_FILE_PATH));
            // convert JSON array to list of users
            infoSaves = new Gson().fromJson(reader, new TypeToken<List<InfoSave>>() {
            }.getType());
        } catch (IOException e) {
            e.printStackTrace();
        }


        //reading the topology
        Supplier<String> vSupplier = new Supplier<>() {
            private int id = 0;

            @Override
            public String get() {
                return "v" + id++;
            }
        };
        Supplier<String> ESupplier = new Supplier<>() {
            private int id = 0;

            @Override
            public String get() {
                return "e" + id++;
            }
        };
        FileReader fileReader;
        DirectedAcyclicGraph<String, String> graph = new DirectedAcyclicGraph<>(vSupplier, ESupplier, false);

        try {

            fileReader = new FileReader(DOT_FILE_PATH);
            DOTImporter<String, String> importer = new DOTImporter<>();
            importer.importGraph(graph, fileReader);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        List<String> spouts = new ArrayList<>();
        List<String> sinks = new ArrayList<>();
        Map<Integer, List<Integer>> father = new HashMap<>();
        Map<Integer, List<Integer>> child = new HashMap<>();
        Iterator<String> itere = new TopologicalOrderIterator<>(graph);
        while (itere.hasNext()) {

            String vertex = itere.next();
            graph.edgesOf(vertex).forEach(e -> {
                if (!graph.getEdgeSource(e).equals(vertex)) {
                    father.computeIfAbsent(Integer.parseInt(vertex.replace("v", "")),
                            v -> new ArrayList<>()).add(Integer.parseInt(graph.getEdgeSource(e).
                            replace("v", "")));
                }

                if (!graph.getEdgeTarget(e).equals(vertex)) {
                    child.computeIfAbsent(Integer.parseInt(vertex.replace("v", "")),
                            v -> new ArrayList<>()).add(Integer.parseInt(graph.getEdgeTarget(e).
                            replace("v", "")));
                }

                if (graph.getAncestors(vertex).size() == 0)
                    spouts.add(vertex);

                if (graph.getDescendants(vertex).size() == 0)
                    sinks.add(vertex);
            });

        }


//        List<Operator> operators = new ArrayList<>();
        HashMap<Integer, Operator> operators = new HashMap<>();
        Iterator<String> itereFinal = new TopologicalOrderIterator<>(graph);
        while (itereFinal.hasNext()) {
            Operator.OperatorType type;
            double incomingMessageSize;
            double outgoingMessageSize;

            String vertex = itereFinal.next();
            if (spouts.contains(vertex)) {
                type = Operator.OperatorType.SOURCE;
                incomingMessageSize = 0;
                outgoingMessageSize = messageSizes.get(1).get(vertex);
            } else if (sinks.contains(vertex)) {
                type = Operator.OperatorType.SINK;
                outgoingMessageSize = 0;
                incomingMessageSize = messageSizes.get(0).get(vertex);
            } else {
                type = Operator.OperatorType.TRANSFORMATION;
                incomingMessageSize = messageSizes.get(0).get(vertex);
                outgoingMessageSize = messageSizes.get(1).get(vertex);
            }
            InfoSave infoSave = null;
            try {
                infoSave = infoSaves.stream().filter(v -> v.getName().equals(vertex)).collect(Collectors.toList()).get(0);
            } catch (Exception e) {
                e.printStackTrace();
            }

            assert infoSave != null;
            operators.put(Integer.parseInt(vertex.replace("v", "")), new Operator(Integer.parseInt(vertex.replace("v", "")), vertex,
                    incomingMessageSize,
                    outgoingMessageSize,
                    infoSave.getInRate(),
                    infoSave.getOutRate(),
                    (int) Math.round(instructionsSize.get(vertex)), type));
        }

        for (Operator operator : operators.values()) {
            String vertex = "v" + operator.getId();

            InfoSave infoSave = infoSaves.stream().filter(v -> v.getName().equals(vertex)).
                    collect(Collectors.toList()).get(0);
            Map<Operator, Double> probabilityRatio = new HashMap<>();

            List<Operator> downstreams = new ArrayList<>();

            for (Map.Entry<String, Double> entry : infoSave.getProbabilityRatio().entrySet()) {
                Operator downstream = operators.get(Integer.parseInt(entry.getKey().replace("v", "")));
                downstreams.add(downstream);

                if (infoSave.getProbabilityRatio().size() == 1) {
                    probabilityRatio.put(downstream, 1.00);
                } else
                    probabilityRatio.put(downstream, entry.getValue());
            }

            List<Integer> upStreamIntegerId = father.get(operator.getId());
            List<Operator> upstreams = new ArrayList<>();
            if (upStreamIntegerId != null)
                for (Integer id : upStreamIntegerId) {
                    upstreams.add(operators.get(id));
                }
            operator.addProperties(downstreams, upstreams, probabilityRatio);
        }
        Map<String, Double> outputDataRate = logAnalysis.outputRate();
        Map<String, Double> inputDatRate = logAnalysis.inputRate();
        for (Map.Entry<Integer, Operator> operatorMap : operators.entrySet()) {
            Operator o = operatorMap.getValue();
            Double outgoingDataRate = outputDataRate.get(o.getName());
            o.setOutgoingDataRate(outgoingDataRate);

            Double incomingDataRate = inputDatRate.get(o.getName());
            o.setIncomingDataRate(incomingDataRate);

            if (Operator.OperatorType.SOURCE == o.getOperatorType())
                INPUT_DATA_RATE.put(o, o.getIncomingDataRate());

        }
//        inputDatRate.forEach((k, v) -> INPUT_DATA_RATE.
//                put(operators.get(Integer.parseInt(k.replace("v", ""))),100.0));
        return new ArrayList<>(operators.values());
    }

    public void calculateAvailableEdgeNodes() {

        for (Operator operator : topology.getOperators()) {
            for (EdgeNode edgeNode : edgeNetwork.getEdgeNodes()) {

                if ((((operator.getOutgoingDataRate() * operator.getOutgoingMessageSize())
                        < edgeNode.getOutgoingBandwidth()) &&
                        (operator.getIncomingDataRate() * operator.getIncomingMessageSize()
                                < edgeNode.getIncomingBandwidth())
//                        || ((1 / operator.getIncomingDataRate())
//                        < (operator.getInstructionSize() / (double) edgeNode.getMIPS()))
                )) {
                    this.operatorsAvailableEdgeNodes.computeIfAbsent(operator, v -> new ArrayList<>()).add(edgeNode);
//                    if (this.operatorsAvailableEdgeNodes.get(operator) == null) {
//                        this.operatorsAvailableEdgeNodes.put(operator, new ArrayList<>());
//                        this.operatorsAvailableEdgeNodes.get(operator).add(edgeNode);
//                    } else
//                        this.operatorsAvailableEdgeNodes.get(operator).add(edgeNode);
                } else
                    this.operatorsAvailableEdgeNodes.computeIfAbsent(operator, v -> new ArrayList<>()).add(null);

            }
        }
    }

    public void runTest() {


        //check if the results contains one resource
        int test = isValid(serverDeployment);
        if (test == -1)
            System.out.println(Double.POSITIVE_INFINITY);

        Topology finalTopology = topology;
        for (int index = 0; index < finalTopology.getOperators().size(); index++) {
            Operator o = finalTopology.getOperators().get(index);
            EdgeNode e = operatorsAvailableEdgeNodes.get(o).get(serverDeployment.get(index) - 1);
            o.setAssignedEdgeNode(e);
            e.setPlacedOperator(o);
        }
//                topology.getOperators().forEach(operator -> {
//
//                    int edgeNodeIndex = args.get(finalTopology.getOperators().indexOf(operator));
//
//                    operator.setAssignedEdgeNode(operatorsAvailableEdgeNodes.get(operator).get(edgeNodeIndex - 1));
//                    operatorsAvailableEdgeNodes.get(operator).get(edgeNodeIndex - 1).setPlacedOperator(operator);
//                });

        List<Double> finishTime = new ArrayList<>();
        for (int i = 0; i < TIMES_OF_RUNS_PER_ASSIGNMENT; i++) {
            pushData(topology);
            finishTime.add(topology.finishTime());
            topology.localResetTopology();
            edgeNetwork.localRestEdgeNetwork();
        }
        topology.resetTopology();
        edgeNetwork.restEdgeNetwork();
        System.out.println(finishTime.stream().mapToDouble(v -> v).average().orElseThrow());
    }

    public static double gcd(double a, double b) {
        if (a < b)
            return gcd(b, a);

        // base case
        if (Math.abs(b) < 0.001)
            return a;

        else
            return (gcd(b, (a - (Math.floor(a / b)))*(b))
            );
    }

    public EdgeNetwork getEdgeNetwork() {
        return edgeNetwork;
    }

    public Topology getTopology() {
        return topology;
    }
}

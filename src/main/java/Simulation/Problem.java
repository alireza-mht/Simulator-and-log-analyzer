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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class Problem {
    private EdgeNetwork edgeNetwork;
    private Topology topology;
    private Map<Operator, List<EdgeNode>> operatorsAvailableEdgeNodes;
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
    private int TEST_BED_INSTRUCTION_SIZE;

    public Problem() throws IOException {

        this.mainProperties = new Properties();

        FileInputStream file;

        //the base folder is ./, the root of the main.properties file
        String path = "./simulator.properties";

        //load the file handle for main.properties
        file = new FileInputStream(path);

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
        DOT_FILE_PATH =mainProperties.getProperty("dot.file.path");
        RESULT_FILE_PATH = mainProperties.getProperty("result.file.path"); //
        TEST_BED_INSTRUCTION_SIZE = Integer.parseInt(mainProperties.getProperty("testbed.instruction.size")); //2451

        this.edgeNetwork = this.generateEdgeNetwork();
        this.topology = this.generateTopology();
        this.operatorsAvailableEdgeNodes = new HashMap<>();
        this.calculateAvalibleEdgeNodes();
    }

    public void solve() {


        int D = topology.getOperators().size();

        Function function = new Function(this.topology.getOperators().size()) {
            @Override
            public double eval(List<Integer> args) throws DimensionsUnmatchedException {


                //check if the results contains one resource
                int test = isValid(args);
                if (test == -1)
                    return Double.POSITIVE_INFINITY;

                Topology finalTopology = topology;
                topology.getOperators().forEach(operator -> {

                    int edgeNodeIndex = args.get(finalTopology.getOperators().indexOf(operator));

                    operator.setAssignedEdgeNode(operatorsAvailableEdgeNodes.get(operator).get(edgeNodeIndex - 1));
                    operatorsAvailableEdgeNodes.get(operator).get(edgeNodeIndex - 1).setPlacedOperator(operator);
                });

                List<Double> finishTime = new ArrayList<>();
                for (int i = 0; i < TIMES_OF_RUNS_PER_ASSIGNMENT; i++) {
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

        System.out.println("Minimum:");
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
                fileWriter.write(  operatorsAvailableEdgeNodes.get(operator).
                        get(solutionsInteger.get(q) - 1).getName()+ " "+ "v" + operator.getId() + "\n");
                q++;
            }
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


        System.out.println(solution);


        int q = 0;
        for (Operator operator : topology.getOperators()) {
            System.out.print(operatorsAvailableEdgeNodes.get(operator).get(solutionsInteger.get(q) - 1).getId() + ", ");
            q++;

        }

    }

    public void pushData(Topology topology) {

        final List<Integer> finish = new ArrayList<>();
        int time = 0;
        double sum = 0;
        int numberOfMessage = 0;
        do {
            time += 1;
            int i = 0;
            for (Operator operator : topology.getSource()) {

                sum += INPUT_DATA_RATE.get(operator);
                numberOfMessage++;
                Event event = new Event(null, operator, (int) Math.floor(sum), 0, time,
                        time);
                if (sum > 1) {
                    sum -= 1;
                    numberOfMessage = 0;
                }
                List<Integer> temp = operator.receiveEvent(event);
                temp = temp.stream().filter(o -> o != -1).collect(Collectors.toList());

                if (temp.size() != 0)
                    temp.stream().filter(o -> topology.getOperatorById(o).getType() == Operator.OperatorType.SINK &&
                            !finish.contains(o)).forEach(finish::add);
                i++;

            }


        } while (!(finish.size() == topology.getSinks().size()));

    }

    //second constraint on the number of threads on teach edge node
    public int isValid(List<Integer> args) {
        List<Integer> edgeNodesIDs = new ArrayList<>();
        // args give us the id of edge node
        for (Operator operator : topology.getOperators()) {

            int id = args.get(topology.getOperators().indexOf(operator));
            edgeNodesIDs.add(operatorsAvailableEdgeNodes.get(operator).get(id - 1).getId());
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

        List<EdgeNode> edgeNodes = generateEdgeNodes(EDGE_SIZE) ;
        return new EdgeNetwork(edgeNodes, generateBandwidth(edgeNodes), generateLatency(edgeNodes));
    }

    public List<EdgeNode> generateEdgeNodes(int size) {
        Map<Pair<EdgeNode, EdgeNode>, Double> latency = new HashMap<>();

        List<EdgeNode> edgeNodes = new ArrayList<>();
        String bandwidthPath = "Bandwidth.txt";
        String hostPath = "pi3-test.txt";
        String threadPath = "thread.txt";

        try {
            int i = 0;
            while(i<size) {
                String line = Files.readAllLines(Path.of("./").resolve(bandwidthPath)).get(i);
                String name = Files.readAllLines(Path.of("./").resolve(hostPath)).get(i);

                //in byte per second
                double inBand = Double.parseDouble(line.split("\t")[0])*1000000/8;
                //latency in second
                double inLatency = Double.parseDouble(line.split("\t")[1])/1000;
                double outBand = Double.parseDouble(line.split("\t")[2])*1000000/8;
                double outLatency = Double.parseDouble(line.split("\t")[3])/1000;

                int thread = Integer.parseInt(Files.readAllLines(Path.of("./").resolve(threadPath)).get(i));

                EdgeNode edgeNode = new EdgeNode(i, name,thread, TEST_BED_INSTRUCTION_SIZE,inBand,outBand, inLatency,outLatency);
                edgeNodes.add(edgeNode);
                i+=1;
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


//            for (String vertexId : infoSave.getProbabilityRatio().keySet()) {
//                Operator temp = operatorList.stream().filter(v -> v.getId() ==
//                        (Integer.parseInt(vertexId.replace("v", "")))).findFirst().orElseThrow();
//                if (infoSave.getProbabilityRatio().keySet().size() == 1)
//                    probabilityRatio.put(temp, 1.00);
//                else
//                    probabilityRatio.put(temp, infoSave.getProbabilityRatio().get("v" + temp.getId()));
//            }

//            List<Integer> downStreamIntegerId = child.get(operator.getId());
//            List<Operator> downstreams;
//            if (downStreamIntegerId != null)
//                downstreams = operators.stream().filter(v -> downStreamIntegerId.contains(v.getId())).
//                        collect(Collectors.toList());
//            else
//                downstreams = new ArrayList<>();

            List<Integer> upStreamIntegerId = father.get(operator.getId());
            List<Operator> upstreams = new ArrayList<>();
            if (upStreamIntegerId != null)
                for (Integer id : upStreamIntegerId) {
                    upstreams.add(operators.get(id));
                }
//            if (upStreamIntegerId != null)
//                upstreams = operators.stream().filter(v -> upStreamIntegerId.contains(v.getId())).
//                        collect(Collectors.toList());
//            else
//                upstreams = new ArrayList<>();

            operator.addProperties(downstreams, upstreams, probabilityRatio);
        }

        Map<String, Double> inputDatRate = logAnalysis.inputRate(spouts);
        inputDatRate.forEach((k, v) -> INPUT_DATA_RATE.
                put(operators.get(Integer.parseInt(k.replace("v", ""))), v));

        return new ArrayList<>(operators.values());
    }

    public void calculateAvalibleEdgeNodes() {

        for (Operator operator : topology.getOperators()) {
            for (EdgeNode edgeNode : edgeNetwork.getEdgeNodes()) {

                if (!(((operator.getOutgoingDataRate() * operator.getOutgoingMessageSize())
                        > edgeNode.getOutgoingBandwidth())
//                        || ((1 / operator.getIncomingDataRate())
//                        < (operator.getInstructionSize() / (double) edgeNode.getMIPS()))
                )) {
                    this.operatorsAvailableEdgeNodes.computeIfAbsent(operator, v -> new ArrayList<>()).add(edgeNode);
//                    if (this.operatorsAvailableEdgeNodes.get(operator) == null) {
//                        this.operatorsAvailableEdgeNodes.put(operator, new ArrayList<>());
//                        this.operatorsAvailableEdgeNodes.get(operator).add(edgeNode);
//                    } else
//                        this.operatorsAvailableEdgeNodes.get(operator).add(edgeNode);
                }

            }
        }
    }

    public static void printTimeInterval(Topology topology) {
        topology.getOperators().forEach(operator -> operator.getRunningIntervals().
                forEach(doubleDoublePair -> System.out.println("Simulation.Operator" + operator.getId() + ":" + doubleDoublePair.getKey()
                        + "-" + doubleDoublePair.getValue())));
    }

}

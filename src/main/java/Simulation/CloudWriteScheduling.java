package Simulation;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.nio.dot.DOTImporter;
import org.jgrapht.traverse.TopologicalOrderIterator;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Supplier;


public class CloudWriteScheduling {

    public static String RESULT_FILE_PATH;
    public static String PI1;
    public static String PI2;
    static String PI3_PATH;
    static String DOT_FILE_PATH;
    static String JSON_FILE_PATH;
    public static String CLOUD_NAME;
    static Properties mainProperties;

    public static void main(String[] args) throws IOException {
        CloudWriteScheduling.readProperties(args[0]);
        Problem problem = new Problem(args[0]);
        Topology topology = problem.getTopology();

        FileWriter fileWriter;
        try {
            String path = PI3_PATH;
            int i = 0 ;
            fileWriter = new FileWriter(RESULT_FILE_PATH);
            List<String> lines = Files.readAllLines(Path.of(path));
            for (Operator operator : topology.getSource()) {
                fileWriter.write(lines.get(i) + " " + operator.getName() + "\n");
                i++;
            }
            for (Operator operator : topology.getSinks()) {
                fileWriter.write(lines.get(i) + " " + operator.getName() + "\n");
                i++;
            }
            for (Operator operator : topology.getOperators()) {
                if (operator.getOperatorType() != Operator.OperatorType.SINK && operator.getOperatorType() != Operator.OperatorType.SOURCE) {
                    fileWriter.write(CLOUD_NAME + " " + operator.getName()+ "\n");
                }
            }

            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    public static void readProperties(String propertyPath) {
        mainProperties = new Properties();
        try {
            FileInputStream file;

            //load the file handle for main.properties
            file = new FileInputStream(propertyPath);

            //load all the properties from this file
            mainProperties.load(file);

            //we have loaded the properties, so close the file handle
            file.close();

            RESULT_FILE_PATH = mainProperties.getProperty("schedule.cloud.result.path");
            PI3_PATH = mainProperties.getProperty("pi.list.path");
            JSON_FILE_PATH = mainProperties.getProperty("json.file.path");
            DOT_FILE_PATH = mainProperties.getProperty("dot.file.path");
//            PI1 = mainProperties.getProperty("res.pi.1");
//            PI2 = mainProperties.getProperty("res.pi.2");
            CLOUD_NAME = mainProperties.getProperty("cloud.name");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void calculateSpouts(){
        // create a reader
//        List<InfoSave> infoSaves = null;
//        Reader reader;
//        try {
////            reader = Files.newBufferedReader(Path.of(JSON_FILE_PATH));
//            // convert JSON array to list of users
////            infoSaves = new Gson().fromJson(reader, new TypeToken<List<InfoSave>>() {
////            }.getType());
//        } catch (IOException e) {
//            e.printStackTrace();
//        }


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
    }
}

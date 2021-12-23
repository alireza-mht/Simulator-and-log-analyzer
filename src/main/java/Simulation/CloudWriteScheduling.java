package Simulation;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;


public class CloudWriteScheduling {

    public static String RESULT_FILE_PATH;
    public static String PI1;
    public static String PI2;
    public static String CLOUD_NAME;
    static Properties mainProperties;

    public static void main(String[] args) throws IOException {
        CloudWriteScheduling.readProperties(args[0]);
        Problem problem = new Problem(args[0]);
        Topology topology = problem.getTopology();
        FileWriter fileWriter;
        try {
            fileWriter = new FileWriter(RESULT_FILE_PATH);
            for (Operator operator : topology.getSource())
                fileWriter.write(PI1 + " " + operator.getName() + "\n");
            for (Operator operator : topology.getSinks())
                fileWriter.write(PI1 + " " + operator.getName() + "\n");

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
            PI1 = mainProperties.getProperty("res.pi.1");
            PI2 = mainProperties.getProperty("res.pi.2");
            CLOUD_NAME = mainProperties.getProperty("cloud.name");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}

package Simulation;

import java.io.IOException;
import java.util.Random;


public class SimulationLauncher {


    public static void main(String[] args) throws IOException {

        //todo: add the resource file and pass it to problem to process it
        Random rand = new Random(2);

        Problem problem = new Problem(args[0]);
//        problem.runTest();
        problem.solve();


    }


}

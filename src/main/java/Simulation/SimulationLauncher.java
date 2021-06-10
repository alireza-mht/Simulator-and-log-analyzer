package Simulation;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;


public class SimulationLauncher {


    public static void main(String[] args) throws IOException {

        //todo: add the resource file and pass it to problem to process it
        Problem problem = new Problem();
        problem.solve();

    }


}

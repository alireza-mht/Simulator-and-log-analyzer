package Simulation.LogAnalysis;

import java.util.ArrayList;
import java.util.List;

public class EdgeOperTaskNames {

    private List<EdgeOperTaskName> edgeTaskBoltNames;

    public EdgeOperTaskNames() {
        this.edgeTaskBoltNames = new ArrayList<>();
    }

    public void add(String edgeName, String taskName, String operatorName) {
        EdgeOperTaskName edgeOperTaskName = new EdgeOperTaskName(edgeName, taskName, operatorName);
        this.edgeTaskBoltNames.add(edgeOperTaskName);
    }

    public String getEdgeFromTask(String task) {
//        for (EdgeNodeTaskName edgeNodeTaskName : edgeTaskBoltNames) {
//            if (edgeNodeTaskName.getTaskName().equals(task))
//                return edgeNodeTaskName.getEdgeName();
//            else return "Not available!";
//        }
        EdgeOperTaskName edgeOperTaskName = edgeTaskBoltNames.stream().filter(pairs -> pairs.getTaskName().equals(task))
                .findAny().orElse(null);
        if (edgeOperTaskName != null) {
            return edgeOperTaskName.getTaskName();
        }else return null;
    }

    public String getTaskFromEdge(String edge){
        EdgeOperTaskName edgeOperTaskName = edgeTaskBoltNames.stream().filter(pairs -> pairs.getEdgeName().equals(edge))
                .findAny().orElse(null);
        if (edgeOperTaskName != null) {
            return edgeOperTaskName.getTaskName();
        }else return null;
    }

    public String getOperatorFromEdge(String edge){
        EdgeOperTaskName edgeOperTaskName = edgeTaskBoltNames.stream().filter(pairs -> pairs.getEdgeName().equals(edge))
                .findAny().orElse(null);
        if (edgeOperTaskName != null) {
            return edgeOperTaskName.getOperatorName();
        }else return null;
    }

    public String getOperatorFromTask(String task){
        EdgeOperTaskName edgeOperTaskName = edgeTaskBoltNames.stream().filter(pairs -> pairs.getTaskName().equals(task))
                .findAny().orElse(null);

        if (edgeOperTaskName != null) {
            return edgeOperTaskName.getOperatorName();
        }else return null;
    }

    public List<String> getTasksName(){
        List<String> tasksName = new ArrayList<>();
        edgeTaskBoltNames.forEach(v -> tasksName.add(v.getTaskName()));
        return tasksName;
    }



    private static class EdgeOperTaskName {
        String edgeName;
        String taskName;
        String operatorName;

        public EdgeOperTaskName(String edgeName, String taskName, String operatorName) {
            this.edgeName = edgeName;
            this.taskName = taskName;
            this.operatorName = operatorName;
        }

        public String getEdgeName() {
            return edgeName;
        }

        public void setEdgeName(String edgeName) {
            this.edgeName = edgeName;
        }

        public String getTaskName() {
            return taskName;
        }

        public void setTaskName(String taskName) {
            this.taskName = taskName;
        }

        public String getOperatorName() {
            return operatorName;
        }

        public void setOperatorName(String operatorName) {
            this.operatorName = operatorName;
        }

    }

}

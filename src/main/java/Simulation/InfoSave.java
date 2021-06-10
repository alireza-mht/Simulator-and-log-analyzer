package Simulation;

import java.util.Map;

public class InfoSave {
    private String name;
    private int inRate;
    private int outRate;
    private Map<String, Double> probabilityRatio;

    public InfoSave(String name, int inRate, int outRate, Map<String, Double> probabilityRatio) {
        this.name = name;
        this.inRate = inRate;
        this.outRate = outRate;
        this.probabilityRatio = probabilityRatio;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getInRate() {
        return inRate;
    }

    public void setInRate(int inRate) {
        this.inRate = inRate;
    }

    public int getOutRate() {
        return outRate;
    }

    public void setOutRate(int outRate) {
        this.outRate = outRate;
    }

    public Map<String, Double> getProbabilityRatio() {
        return probabilityRatio;
    }

    public void setProbabilityRatio(Map<String, Double> probabilityRatio) {
        this.probabilityRatio = probabilityRatio;
    }
}

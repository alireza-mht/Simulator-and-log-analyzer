package Simulation.GWO;

import java.util.List;

/**
 * Simulation.GWO.WolfPackSolution contains information about solution to the particular problem, including best wolf and value progression
 *
 * @author Matt
 */
public class WolfPackSolution {
    private Wolf bestWolf;
    private List<Double> progression;
    private Function f;

    public WolfPackSolution(Wolf bestWolf, List<Double> progression, Function f) {
        this.bestWolf = bestWolf;
        this.progression = progression;
        this.f = f;
    }

    public int iterationsPassed() {
        return progression.size();
    }

    public List<Double> getProgression() {
        return progression;
    }

    public Wolf getBestWolf() {
        return bestWolf;
    }

    public double solution() {
        return progression.get(iterationsPassed() - 1);
    }

    public double bestSolution() {
        return f.eval(bestWolf.getPos());
    }

    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append("Best of each iteration\n");
        for (int i = 0; i < progression.size(); i++) {
            str.append(i + 1).append("#: ").append(progression.get(i)).append("\n");
        }
//		str.append("Best Simulation.GWO.Wolf: ").append(bestWolf).append("\n");
        str.append("Best Solution: ").append(bestSolution()).append("\n\n");

        return str.toString();
    }

    public List<Integer> bestSolutionInteger() {
        return bestWolf.getPos();
    }
}

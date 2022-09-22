package Simulation.GWO;

import java.util.*;

/**
 * @author Matt
 */
public class WolfPack {
    private List<Wolf> pack;
    Wolf wAlpha = null;
    Wolf wBeta = null;
    Wolf wDelta = null;
    Wolf best;
    double wAlphaBest = Double.POSITIVE_INFINITY;
    double wBetaBest = Double.POSITIVE_INFINITY;
    double wDeltaBest = Double.POSITIVE_INFINITY;
    List<Integer> lLimits, uLimits;

    /**
     * Empty constructor
     */
    public WolfPack() {

    }

    /**
     * Initializes wolf pack
     *
     * @param N       - number of wolves
     * @param D       - Number of dimensions of loaded function
     * @param lLimits - lower bounds
     * @param uLimits - upper bounds
     */
    private void initializePack(int N, int D, List<Integer> lLimits, List<Integer> uLimits) {
        Random rand = new Random();
        pack = new ArrayList<>();
        this.lLimits = lLimits;
        this.uLimits = uLimits;

        for (int i = 0; i < N; i++) {
            List<Integer> pos = new ArrayList<>();
            for (int j = 0; j < D; j++) {
                int P = (int) (rand.nextDouble() * (uLimits.get(j) - lLimits.get(j)) + lLimits.get(j));
                if (!pos.contains(P))
                    pos.add(P);
                else
                    j--;
            }
            pack.add(new Wolf(pos));
        }
        wAlpha = null;
        wBeta = null;
        wDelta = null;
        wAlphaBest = Double.POSITIVE_INFINITY;
        wBetaBest = Double.POSITIVE_INFINITY;
        wDeltaBest = Double.POSITIVE_INFINITY;

    }

    private void resetWolfBests(double val) {
        wAlphaBest = val;
        wBetaBest = val;
        wDeltaBest = val;
    }

    /**
     * Assigns new three best wolves in a pack, default less than comparator
     *
     * @param f - Simulation.GWO.Function loaded into function
     */
    private void chooseLeadingWolves(Function f) {
        Comparator comp = (x, y) -> x < y;

        chooseLeadingWolves(f, comp);
    }
int m=0;
    /**
     * Assigns new three best wolves in a pack, according to provided comparator
     *
     * @param f    - Simulation.GWO.Function loaded into function
     * @param comp - object Simulation.GWO.Comparator that acts as a comparing mechanism
     */
    private void chooseLeadingWolves(Function f, Comparator comp) {

        for (Wolf w : pack) {
            double fVal = f.eval(w.getPos());
            if (comp.compare(fVal, wAlphaBest)) {
                wAlpha = w;
                wAlphaBest = fVal;
            }
        }
        for (Wolf w : pack) {
            double fVal = f.eval(w.getPos());
            if (comp.compare(fVal, wBetaBest) && w != wAlpha) {
                wBeta = w;
                wBetaBest = fVal;
            }
        }
        for (Wolf w : pack) {
            double fVal = f.eval(w.getPos());
            if (comp.compare(fVal, wDeltaBest) && w != wBeta && w != wAlpha) {
                wDelta = w;
                wDeltaBest = fVal;
            }
        }

        try {

            wAlpha = new Wolf(wAlpha);
            wBeta = new Wolf(wBeta);
            wDelta = new Wolf(wDelta);
        } catch (Exception e) {
//            System.out.println("No solution found");
            throw new NullPointerException("No solution found");
        }

    }

    /**
     * Moves the given wolf in the pack, hunting behavior
     *
     * @param w      - Simulation.GWO.Wolf to be moved
     * @param alphaA - a coefficient, dictating how to move the wolf
     * @param betaA  - a coefficient, dictating how to move the wolf
     * @param deltaA - a coefficient, dictating how to move the wolf
     */
    private void moveTheWolf(Wolf w, double alphaA, double betaA, double deltaA) {
        Random rand = new Random();
        for (int j = 0; j < w.getPos().size(); j++) {

            /**
             * Move the wolves
             */
            double r1 = rand.nextDouble();
            double r2 = rand.nextDouble();

            double A1 = 2 * alphaA * r1 - alphaA;
            double C1 = 2 * r2;
            double DAlpha = Math.abs(C1 * wAlpha.posAtIndex(j) - w.posAtIndex(j));
            double X1 = wAlpha.posAtIndex(j) - A1 * DAlpha;

            r1 = rand.nextDouble();
            r2 = rand.nextDouble();

            double A2 = 2 * betaA * r1 - betaA;
            double C2 = 2 * r2;
            double DBeta = Math.abs(C2 * wBeta.posAtIndex(j) - w.posAtIndex(j));
            double X2 = wBeta.posAtIndex(j) - A2 * DBeta;

            r1 = rand.nextDouble();
            r2 = rand.nextDouble();

            double A3 = 2 * deltaA * r1 - deltaA;
            double C3 = 2 * r2;
            double DDelta = Math.abs(C3 * wDelta.posAtIndex(j) - w.posAtIndex(j));
            double X3 = wDelta.posAtIndex(j) - A3 * DDelta;

            w.setAtIndex(j, (int) ((X1 + X2 + X3) / 3));
        }
    }

    /**
     * Trims the wolf pack back into the limits
     */
    private void trimToLimits() {
        for (Wolf w : pack) {
            for (int j = 0; j < w.getPos().size(); j++) {
                if (w.posAtIndex(j) < lLimits.get(j)) {
                    w.setAtIndex(j, lLimits.get(j));
                }
                if (w.posAtIndex(j) > uLimits.get(j)) {
                    w.setAtIndex(j, uLimits.get(j));
                }
            }
        }
    }

    /**
     * Finds a global minimum of a function on a given domain
     *
     * @param f          - Simulation.GWO.Function to be processed
     * @param parameters - Specially prepared parameter class for wolf pack problems
     * @return the best solution produced by the algorithm, packaged as special solution object
     * @throws DimensionsUnmatchedException throw exception related to the dimensions
     */
    public WolfPackSolution findMinimum(Function f, WolfPackParameters parameters) throws DimensionsUnmatchedException, NullPointerException {

        double MaxA = 2.0;
        List<Double> progression = new ArrayList<>();

        initializePack(parameters.getWolfCount(), parameters.getDimensions(), parameters.getLLimits(), parameters.getULimits());
        resetWolfBests(Double.POSITIVE_INFINITY);
        chooseLeadingWolves(f);

        int I = parameters.getIterations();

        //Improved Simulation.GWO
        double aMax = 2.0;
        double aMin = 2 - (I - 1) * (2 / (double) I);
        double ln = Math.log10((aMin / aMax)) / Math.log(2);


        for (int h = 0; h < I; h++) {

            //Simple Simulation.GWO
            double a = MaxA - h * MaxA / I;

            //Improved Simulation.GWO
            double alphaA = aMax * Math.exp(Math.pow(((h + 1) / (double) I), 2) * ln);
            double betaA = aMax * Math.exp(Math.pow(((h + 1) / (double) I), 3) * ln);
            double deltaA = (alphaA + betaA) / 2;
            //we will find all the new positions for all the wolfs
            for (Wolf wolf : pack) {
                moveTheWolf(wolf, alphaA, betaA, deltaA);
            }
            trimToLimits();
            resetWolfBests(Double.POSITIVE_INFINITY);
            chooseLeadingWolves(f);
            progression.add(f.eval(wAlpha.getPos()));

            if (best == null)
                best = wAlpha;

            if (f.eval(wAlpha.getPos()) < f.eval(best.getPos()))
                best = wAlpha;

        }

        return new WolfPackSolution(best, progression, f);
    }

}

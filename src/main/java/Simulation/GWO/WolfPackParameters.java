package Simulation.GWO;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * Simulation.GWO.WolfPackParameters contains information about the wolf pack for a given problem.
 * @author Matt
 *
 */
public class WolfPackParameters {
	private int N = 20;
	private int D ;
	private int I = 30;
	
	private List<Integer> uLimits;
	private List<Integer> lLimits;
	
	public WolfPackParameters(int dimensions){
		D = dimensions;
		uLimits = new ArrayList<>(Collections.nCopies(D, 1));
		lLimits = new ArrayList<>(Collections.nCopies(D, 0));

	}
	
	public int getIterations() {return I;}
	public int getDimensions() {return D;}
	public int getWolfCount() {return N;}
	
	public List<Integer> getULimits() { return uLimits;}
	public List<Integer> getLLimits() { return lLimits;}
	
	public void setPackParameters(int packSize, int iterations) {
		I = iterations;
		N = packSize;
	}
	public void setLimits(List<Integer> lLimits, List<Integer> uLimits) {
		if(uLimits.size() != D || lLimits.size() != D) {
			throw new DimensionsUnmatchedException();
		}
		this.uLimits = uLimits;
		this.lLimits = lLimits;
	}
	
	
}

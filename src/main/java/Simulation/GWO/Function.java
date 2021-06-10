package Simulation.GWO;

import java.util.List;


public abstract class Function{
	protected int D;
	
	public Function(int D){
		this.D = D;
	}
	
	protected void checkDimensions(List<Integer> args) throws DimensionsUnmatchedException {
		if(D != args.size())
			throw new DimensionsUnmatchedException();
	}
	
	public abstract double eval(List<Integer> args) throws DimensionsUnmatchedException;
}

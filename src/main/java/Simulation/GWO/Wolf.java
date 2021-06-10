package Simulation.GWO;

import java.util.ArrayList;
import java.util.List;

public class Wolf {
	private List<Integer> pos;
	
	public Wolf(List<Integer> pos) {
		this.pos = new ArrayList<>();
		for(Integer d : pos) {
			this.pos.add( d);
		}
	}
	
	public Wolf(Wolf w) {
		this.pos = new ArrayList<>();
		for(Integer d : w.getPos()) {
			this.pos.add( d);
		}
	}
	
	public void setAtIndex(int i, int p) {
		pos.set(i, p);
	}
	
	public double posAtIndex(int i) {
		return pos.get(i);
	}
	
	public List<Integer> getPos(){
		return pos;
	}
	
	public String toString() {
		return pos.toString();
	}
	
}

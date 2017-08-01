package bigdata2.bigdata2;


import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class VertexState extends IntWritable {
	// deve estendere writable, ma della stampa su file me ne occupero' dopo TODO

	private IntArrayListWritable myCliques;
	private IntArrayListWritable cliquesAdmin; // le cricche di cui sono admin
	private List<Text> myFakeVertices;
	private List<Integer> mySubgraphs;
	
	public VertexState() {
		myFakeVertices = new ArrayList<>();
	}

	public IntArrayListWritable getMyCliques() {
		return myCliques;
	}

	public void setMyCliques(IntArrayListWritable myCliques) {
		this.myCliques = myCliques;
	}

	public IntArrayListWritable getCliquesAdmin() {
		return cliquesAdmin;
	}

	public void setCliquesAdmin(IntArrayListWritable cliquesAdmin) {
		this.cliquesAdmin = cliquesAdmin;
	}

	public List<Text> getMyFakeVertices() {
		return myFakeVertices;
	}

	public void setMyFakeVertices(List<Text> myFakeVertices) {
		this.myFakeVertices = myFakeVertices;
	}

	public void addFakeVertex(Text fakeVertexId) {
		this.myFakeVertices.add(fakeVertexId);
		
	}


	public List<Integer> getMySubgraphs() {
		return mySubgraphs;
	}

	public void setMySubgraphs(List<Integer> mySubgraphs) {
		this.mySubgraphs = mySubgraphs;
	}
	
	
}

package bigdata2.bigdata2;


import org.apache.giraph.aggregators.Aggregator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class MegaCliquesAggregator implements Aggregator<MapWritable>{

	private MapWritable value;
	private IntWritable count; // contatore per il valore della prossima chiave

	public MapWritable createInitialValue() {
		this.value = new MapWritable();
		this.count = new IntWritable(1);
		return this.value;
	}


	public void aggregate(MapWritable value) {
		for(Writable val : value.values()) {
			this.value.put(nextKey(),val); // nel set usero' sempre la chiave zero
		}
		
	}
	
	private IntWritable nextKey() {
		int key = this.count.get();
		this.count = new IntWritable(key+1);
		return new IntWritable(key);
	}

	public void setAggregatedValue(MapWritable value) {
		this.value = value;
	}

	public MapWritable getAggregatedValue() {
		return this.value;
	}
	
	public void reset() {
		
	}

}

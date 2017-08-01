package bigdata2.bigdata2;

import java.util.HashMap;
import java.util.Map;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class MyMasterCompute extends DefaultMasterCompute {

	private Text agg1;
	
	public MyMasterCompute() {
		this.agg1 = new Text("MegaCliquesAgg");
	}
	
	@Override
	public void initialize() throws InstantiationException, IllegalAccessException {
		// Initialization phase, used to initialize aggregator/Reduce/Broadcast
		// or to initialize other objects

		registerPersistentAggregator(agg1.toString(), MegaCliquesAggregator.class);
	}
	
    @Override
    public final void compute() {
        if(getSuperstep()==4) { // ho creato le liste delle megacricche, le rendo globali
        	MapWritable megaCliques = ((MapWritable) getAggregatedValue(agg1.toString()));
        	System.out.println("megaCliques:");
        	for(Writable val : megaCliques.values()) {
        		System.out.println(val);
        	}
        	
        	// ora converto la mappa int -> Text in una mappa megacriccaId -> lista cricche
			Map<Integer,IntArrayListWritable> megaCliquesMap = new HashMap<Integer,IntArrayListWritable>();
			IntArrayListWritable arrayList;
			int i = 1;
			for(Writable t : megaCliques.values()) {
				String content = ((Text) t).toString();
				content = content.substring(1, content.length()-1); // tolgo le parentesi quadre
				String[] values = content.split(", ");
				arrayList = new IntArrayListWritable();
				for(String val : values) 
					arrayList.add(new IntWritable(Integer.parseInt(val)));
				megaCliquesMap.put(i, arrayList);
				i++;
			}
			CreateSubgraphs.megaCliques = megaCliquesMap;
        }

    }
}

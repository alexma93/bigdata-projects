package mapReduce;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import bigdata2.bigdata2.CreateSubgraphs;

public class SortReducer extends
		Reducer<Text, Text, Text, Text> {
	
	
	public void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {

		Integer subgraphId = Integer.parseInt(key.toString());
		String subgraph = CreateSubgraphs.megaCliques.get(subgraphId).toString();
		context.write(new Text("Subgraph: "+key.toString()),new Text("Cliques: "+subgraph));
		List<Text> adiacenceLists = new LinkedList<Text>();
		
		for(Text t : values)
			adiacenceLists.add(new Text(t));
		Collections.sort(adiacenceLists);
		for(Text v : adiacenceLists)
			context.write(v,null);
	}


}








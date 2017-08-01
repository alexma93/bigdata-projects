package amazon3;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SimilarUsersCombiner2 extends
		Reducer<Text, Text, Text, Text> {
	// combiner, dopo la seconda Map
	
	
	public void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		
		String output = "";
		for (Text value : values)
			output += value.toString()+"\t";
		
		output.substring(0, output.length()-1); // tolgo \t finale
		context.write(key,new Text(output));
	}

}








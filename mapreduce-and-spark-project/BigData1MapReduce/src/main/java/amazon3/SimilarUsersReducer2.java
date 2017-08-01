package amazon3;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SimilarUsersReducer2 extends
		Reducer<Text, Text, Text, Text> {
	
	
	public void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		
		List<String> products = new LinkedList<>();
		
		// faccio uno split, perchè c'è stato il combiner prima che può aver accorpato piu prodotti
		for (Text value : values)
			for(String val: value.toString().split("\t"))
				products.add(val);
		
		
		if(products.size()>=3)
			context.write(key,new Text(products.toString()));
	}

}








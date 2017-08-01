package mapReduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class SortMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] splitted = line.split("\t");
		String subgraph = splitted[0].split("-")[1];
		
		context.write(new Text(subgraph), new Text(line));
		}

	
}
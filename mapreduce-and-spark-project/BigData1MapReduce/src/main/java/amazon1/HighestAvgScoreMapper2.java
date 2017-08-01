package amazon1;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class HighestAvgScoreMapper2 extends
		Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();

		String[] fields = line.split("\t");
		String month = fields[0];
		String idProd = fields[1];
		String score = fields[2];
		context.write(new Text(month), new Text(idProd+"\t"+score));
		}

	
}
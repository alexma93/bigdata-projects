package amazon3;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class SimilarUsersMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();

		String[] fields = line.split("\t");
		String idProd = fields[1];
		String idUser = fields[2];
		int score = Integer.parseInt(fields[6]);
		if (score >= 4)
			context.write(new Text(idProd), new Text(idUser));
		}

	
}




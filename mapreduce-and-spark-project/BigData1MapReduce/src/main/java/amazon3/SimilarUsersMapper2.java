package amazon3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class SimilarUsersMapper2 extends
		Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();

		String[] fields = line.split("\t");
		String idProd = fields[0];
		String idUser1 = fields[1];
		String idUser2 = fields[2];
		context.write(new Text(idUser1+"\t"+idUser2), new Text(idProd));
		}

	
}




package amazon1;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class HighestAvgScoreMapper extends
		Mapper<LongWritable, Text, Text, DoubleWritable> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();

		String[] fields = line.split("\t");
		String idProd = fields[1];
		DoubleWritable score = new DoubleWritable(Double.parseDouble(fields[6]));
		Date date = new Date(Long.parseLong(fields[7])*1000L);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM");
		String dateString = sdf.format(date);
		context.write(new Text(dateString+"\t"+idProd), score);
		}

	
}
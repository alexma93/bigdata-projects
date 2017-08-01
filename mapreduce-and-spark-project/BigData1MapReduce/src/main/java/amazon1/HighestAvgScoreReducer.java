package amazon1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HighestAvgScoreReducer extends
Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	public void reduce(Text key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {

		double avg = 0;
		int len = 0;
		for (DoubleWritable value : values) {
			avg += value.get();
			len++;
		}
		avg = avg/len;
		context.write(key, new DoubleWritable(avg));
	}

}








package amazon1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class HighestAvgScore {
	/* Un job che sia in grado di generare, per ciascun mese, i cinque prodotti che hanno ricevuto lo
score medio più alto, indicando ProductId e score medio e ordinando il risultato
temporalmente. */

	public static void main(String[] args) throws Exception {
		double start = System.currentTimeMillis();

		Job job = new Job(new Configuration(), "HighestAvgScore");

		job.setJarByClass(HighestAvgScore.class);

		job.setMapperClass(HighestAvgScoreMapper.class);
		job.setReducerClass(HighestAvgScoreReducer.class);


		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path output = new Path(args[1]);
		FileOutputFormat.setOutputPath(job,output);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.waitForCompletion(true);

		Job job2 = new Job(new Configuration(), "HighestAvgScore2");

		job2.setJarByClass(HighestAvgScore.class);		
		job2.setMapperClass(HighestAvgScoreMapper2.class);
		job2.setReducerClass(HighestAvgScoreReducer2.class);


		FileInputFormat.addInputPath(job2, output);
		FileOutputFormat.setOutputPath(job2, new Path(args[1]+"_final"));

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);

		job2.waitForCompletion(true);

		double endTime = (System.currentTimeMillis() - start) / 1000;
		System.out.println("Tempo impiegato(s):\t" + endTime);
	}
}
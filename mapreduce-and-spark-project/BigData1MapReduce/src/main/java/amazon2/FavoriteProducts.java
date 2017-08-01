package amazon2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FavoriteProducts {
	/* Un job che sia in grado di generare, per ciascun utente, i 10 prodotti preferiti (ovvero quelli
	che ha recensito con il punteggio più alto), indicando ProductId e Score. Il risultato deve essere
	ordinato in base allo UserId.
	 */
	public static void main(String[] args) throws Exception {
		double start = System.currentTimeMillis();
		Job job = new Job(new Configuration(), "FavoriteProducts");

		job.setJarByClass(FavoriteProducts.class);

		job.setMapperClass(FavoriteProductsMapper.class);
		job.setReducerClass(FavoriteProductsReducer.class);


		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.waitForCompletion(true);

		double endTime = (System.currentTimeMillis() - start) / 1000;
		System.out.println("Tempo impiegato(s):\t" + endTime);
	}
}
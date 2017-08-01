package amazon3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SimilarUsers {
	/*[Facoltativo] Un job in grado di generare coppie di utenti con gusti affini, dove due utenti
	hanno gusti affini se hanno recensito con score superiore o uguale a 4 almeno tre prodotti in
	comune, indicando le coppie di utenti e i prodotti recensiti che condividono Il risultato deve
	essere ordinato in base allo UserId del primo elemento della coppia e, possibilmente, non
	deve presentare duplicati.
	 */

	public static void main(String[] args) throws Exception {
		double start = System.currentTimeMillis();
		
		Job job = new Job(new Configuration(), "SimilarUsers");

		job.setJarByClass(SimilarUsers.class);
		
		job.setMapperClass(SimilarUsersMapper.class);
		job.setReducerClass(SimilarUsersReducer.class);
		

		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path output = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, output);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);
		
		Job job2 = new Job(new Configuration(), "SimilarUsers2");
		
		job2.setJarByClass(SimilarUsers.class);		
		job2.setMapperClass(SimilarUsersMapper2.class);
		job2.setCombinerClass(SimilarUsersCombiner2.class);
		job2.setReducerClass(SimilarUsersReducer2.class);
		

		FileInputFormat.addInputPath(job2, output);
		FileOutputFormat.setOutputPath(job2, new Path(args[1]+"_final"));

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		job2.waitForCompletion(true);
		
		double endTime = (System.currentTimeMillis() - start) / 1000;
		System.out.println("Tempo impiegato(s):\t" + endTime);
		
	}
}
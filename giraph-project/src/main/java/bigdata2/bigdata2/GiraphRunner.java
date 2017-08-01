package bigdata2.bigdata2;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.*;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import mapReduce.SortMapper;
import mapReduce.SortReducer;


public class GiraphRunner implements Tool{

    private Configuration conf;
    public Configuration getConf() {
        return conf;
    }
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @SuppressWarnings("static-access")
	public int run(String[] args) throws Exception {
    	CreateSubgraphs.cliqueFile = args[0];
        String inputPath=args[1];
        String outputPath= args[2];
        GiraphConfiguration giraphConf = new GiraphConfiguration(getConf());
        giraphConf.setComputationClass(CreateSubgraphs.class); 
        giraphConf.setEdgeInputFormatClass(NdeEdgeInputFormat.class);
        GiraphFileInputFormat.addEdgeInputPath(giraphConf, new Path(inputPath));
        giraphConf.setVertexOutputFormatClass(AdjacencyListTextVertexOutputFormat.class);
        giraphConf.setLocalTestMode(false);
        giraphConf.setWorkerConfiguration(1, 1, 100);
        giraphConf.SPLIT_MASTER_WORKER.set(giraphConf, false);
        giraphConf.setMasterComputeClass(MyMasterCompute.class);
        //giraphConf.set("mapreduce.framework.name", "yarn");
        //giraphConf.set("mapreduce.jobtracker.address", "sc2-rm1:8088");
        giraphConf.setNumComputeThreads(4);
        InMemoryVertexOutputFormat.initializeOutputGraph(giraphConf);
        GiraphJob giraphJob = new GiraphJob(giraphConf,"CreateSubgraphs");
        FileOutputFormat.setOutputPath(giraphJob.getInternalJob(), new Path(outputPath));
        giraphJob.run(true);
        
		@SuppressWarnings("deprecation")
		double start = System.currentTimeMillis();
		Job job = new Job(new Configuration(), "FavoriteProducts");

		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);

		FileInputFormat.addInputPath(job, new Path(outputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath+"_sorted"));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);
		
		double endTime = (System.currentTimeMillis() - start) / 1000;
		System.out.println("Tempo impiegato(s) MapReduce:\t" + endTime);

        return 0;
    }

    public static void main(String[] args) throws Exception{
        ToolRunner.run(new GiraphRunner(), args);
    }
}
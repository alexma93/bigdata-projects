package Amazon1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


public class Amazon1 {
	/* Un job che sia in grado di generare, per ciascun mese, i cinque prodotti che hanno ricevuto lo
	score medio più alto, indicando ProductId e score medio e ordinando il risultato
	temporalmente.
	 */

	public static void main(String[] args) {
		double start = System.currentTimeMillis();

		String inputFile = args[0];
		String outputFile = args[1];

		SparkConf conf = new SparkConf().setAppName("Amazon1");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> input = sc.textFile(inputFile);

		JavaPairRDD<Product,Tuple2<Double,Integer>> mapper = input.mapToPair( line -> {
			String[] splitted = line.split("\t");
			String idProd = splitted[1];
			double score = Integer.parseInt(splitted[6]);
			Date date = new Date(Long.parseLong(splitted[7])*1000L);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM");
			String dateString = sdf.format(date);
			return new Tuple2<>(new Product(idProd,dateString),new Tuple2<Double,Integer>(score,1));
		});

		JavaPairRDD<String,Double> result = mapper.reduceByKey((a,b) -> new Tuple2<>(a._1+b._1,a._2+b._2))
				.mapValues(tuple -> tuple._1/tuple._2)
				.mapToPair(kv -> new Tuple2<>(kv._1.getMonth(),new Tuple2<String,Double>(kv._1.getIdProd(),kv._2)))
				// la chiave e' solo il mese
				.groupByKey().sortByKey().flatMapToPair(kv -> {
					// lista di tuple (idProdotto,avgScore) che conservera' i 5 prodotti con score piu alto
					List<Tuple2<String,Double>> top = new ArrayList<>(6);
					String month = kv._1;
					for(Tuple2<String,Double> idAndScore : kv._2)
						addProd(top,month,idAndScore);
					return top.iterator();
				});

		result.saveAsTextFile(outputFile);

		sc.close();
		double endTime = (System.currentTimeMillis() - start) / 1000;
		System.out.println("Tempo impiegato(s):\t" + endTime);
	}

	private static void addProd(List<Tuple2<String, Double>> topFive, String month, Tuple2<String, Double> idAndScore) {
		int i = topFive.size()-1;

		while(i>=0 && topFive.get(i)._2 < idAndScore._2)
			i--;
		i++;
		if ( i <= 4 ) {
			topFive.add(i, new Tuple2<>(month+"\t"+idAndScore._1,idAndScore._2));
			if(topFive.size()>5)
				topFive.remove(5);
		}
	}


}

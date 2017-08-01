package Amazon2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import Amazon2.Product;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;


public class Amazon2 {

	public static void main(String[] args) {
		double start = System.currentTimeMillis();

		String inputFile = args[0];
		String outputFile = args[1];

		SparkConf conf = new SparkConf().setAppName("Amazon1");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> input = sc.textFile(inputFile);

		JavaPairRDD<String,Tuple2<String,Double>> mapper = input.mapToPair( line -> {
			String[] splitted = line.split("\t");
			String idUser = splitted[2];
			String idProd = splitted[1];
			double score = Integer.parseInt(splitted[6]);
			return new Tuple2<>(idUser,new Tuple2<String,Double>(idProd,score));
		});

		JavaPairRDD<String, Double> result = mapper.groupByKey().sortByKey()
				.flatMapToPair(keyVal -> {
					List<Tuple2<String,Double>> favourite10Products = new ArrayList<>(10);
					Map<String,Double> idProd2Scores = new TreeMap<>();
					for(Tuple2<String,Double> val : keyVal._2){
						Product p = new Product(val._1,val._2);
						addProd(favourite10Products,p,keyVal._1,idProd2Scores);
					}
					return favourite10Products.iterator();
				});

		result.saveAsTextFile(outputFile);

		sc.close();
		double endTime = (System.currentTimeMillis() - start) / 1000;
		System.out.println("Tempo impiegato(s):\t" + endTime);
	}

	private static void addProd(List<Tuple2<String, Double>> favourite10Products, Product p, String userID, Map<String,Double> idProd2Scores) {
		int i = favourite10Products.size()-1;
		while(i>=0 && favourite10Products.get(i)._2 < p.getScore())
			i--;
		i++;
		if ( i <= 9 ) {
			if (!idProd2Scores.containsKey(p.getIdProd())) {
				favourite10Products.add(i, new Tuple2<>(userID+"\t"+p.getIdProd(), p.getScore()));
				idProd2Scores.put(p.getIdProd(), p.getScore());
			}
			else {
				if (idProd2Scores.get(p.getIdProd())<p.getScore()) {
					favourite10Products.remove(i);
					idProd2Scores.remove(p.getIdProd());
					addProd(favourite10Products,p,userID,idProd2Scores);
				}
			}
			if(favourite10Products.size()>10)
				favourite10Products.remove(10);
		}
	}


}

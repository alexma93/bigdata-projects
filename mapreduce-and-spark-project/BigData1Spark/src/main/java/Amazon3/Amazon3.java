package Amazon3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import java.util.Iterator;


public class Amazon3 {

	public static void main(String[] args) {
		double start = System.currentTimeMillis();

		String inputFile = args[0];
		String outputFile = args[1];

		SparkConf conf = new SparkConf().setAppName("Amazon3");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> input = sc.textFile(inputFile);

		JavaPairRDD<String,String> mapper = input.filter(line -> Integer.parseInt(line.split("\t")[6])>=4).mapToPair( line -> {
			String[] splitted = line.split("\t");
			String idUser = splitted[2];
			String idProd = splitted[1];
			return new Tuple2<>(idProd,idUser);
		}).distinct();
		
		JavaPairRDD<String, Iterable<String>> result = mapper.join(mapper).filter(prod22users -> prod22users._2._1.compareTo(prod22users._2._2) < 0)
				.mapToPair(prod22users -> new Tuple2<>(new Tuple2<>(prod22users._2._1,prod22users._2._2), prod22users._1))
				.groupByKey().filter(users2prods -> {
					int size = 0;
					Iterator<String> it = users2prods._2.iterator();
					while(it.hasNext()) {
						String prod = it.next();
						size++;
					}
					return size >=3;
				}).mapToPair(kv -> new Tuple2<>(kv._1._1+"\t"+kv._1._2,kv._2)).sortByKey();
				

		result.saveAsTextFile(outputFile);

		sc.close();
		double endTime = (System.currentTimeMillis() - start) / 1000;
		System.out.println("Tempo impiegato(s):\t" + endTime);
	}

}

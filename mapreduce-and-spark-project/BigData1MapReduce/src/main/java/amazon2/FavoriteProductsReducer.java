package amazon2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FavoriteProductsReducer extends
		Reducer<Text, Text, Text, IntWritable> {
	
	
	public void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		String[] val;
		String idProd;
		int score;

		String user = key.toString();
		// uso un treeSet, cosi i prodotti sono gia' ordinati per score
		Set<ScoreProduct> topProducts = new TreeSet<ScoreProduct>(Collections.reverseOrder());
		
		for (Text value : values) {
			val = value.toString().split("\t");
			idProd = val[0];
			score = Integer.parseInt(val[1]);
			topProducts.add(new ScoreProduct(idProd,score));
			
		}
		
		topProducts = getTopTen(topProducts);
		
		for(ScoreProduct p : topProducts)
			context.write(new Text(user+"\t"+p.getId()), new IntWritable(p.getScore()));
	}

	private Set<ScoreProduct> getTopTen(Set<ScoreProduct> topProducts) {
		// prendo i primi 10 e gestisco i duplicati (prodotti con lo stesso id)
		Set<ScoreProduct> newTopProducts = new TreeSet<ScoreProduct>(Collections.reverseOrder());
		List<String> productIds = new ArrayList<String>(10);
		int i = 0;
		for(ScoreProduct p : topProducts) {
			if (i==10)
				break;
			if (!productIds.contains(p.getId())) {
				productIds.add(p.getId());
				newTopProducts.add(p);
				i++;
			}
			
		}
		return newTopProducts;
	}

}








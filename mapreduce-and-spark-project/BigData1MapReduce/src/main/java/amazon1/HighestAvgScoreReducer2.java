package amazon1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HighestAvgScoreReducer2 extends
		Reducer<Text, Text, Text, DoubleWritable> {
	
	public void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		String[] splitted;
		String idProd;
		double score;
		
		List<Product> topFive = new ArrayList<>(6);
		
		for (Text value : values) {
			splitted = value.toString().split("\t");
			idProd = splitted[0];
			score = Double.parseDouble(splitted[1]);
			addToTopFive(topFive,new Product(idProd,score));
		}
		
		for(Product p : topFive)
			context.write(new Text(key+"\t"+p.getIdProd()),new DoubleWritable(p.getScore()));
	}

	private void addToTopFive(List<Product> topFive, Product p) {
		/* aggiunge un prodotto alla lista topFive, mantenendola ordinata in base allo score
		 * e di dimensione massima 5
		 */
		int i = topFive.size()-1;
		while(i>=0 && topFive.get(i).getScore() <= p.getScore()) {
			Product next = topFive.get(i);
			// una if per ordinare alfabeticamente i prodotti (non richiesto)
			if(next.getScore() == p.getScore() && next.getIdProd().compareTo(p.getIdProd())<0)
				break;
			i--;
		}
		i++;
		if ( i <= 4 ) {
			topFive.add(i, p);
			if(topFive.size()>5)
				topFive.remove(5);
		}
	}
}








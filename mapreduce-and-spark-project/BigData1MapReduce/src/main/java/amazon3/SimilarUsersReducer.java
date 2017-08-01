package amazon3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SimilarUsersReducer extends
		Reducer<Text, Text, Text, Text> {
	
	
	public void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {

		String idUser;
		
		// elimino i duplicati
		Set<String> usersSet = new HashSet<>();
		for (Text value : values) {
			idUser = value.toString();
			usersSet.add(idUser);			
		}
		List<String> users = new ArrayList<>(usersSet);
		
		List<Pair<String,String>> couples = getAllCouples(users);	
		
		
		for( Pair<String,String> c : couples)
			context.write(key, new Text(c.getFirst()+"\t"+c.getSecond()));
	}

	private List<Pair<String,String>> getAllCouples(List<String> users) {
		/* genero tutte le coppie possibili a partire dalla lista data, 
		senza creare coppie duplicate e ordinando la coppia alfabeticamente */
		int i;
		String user2;
		Pair<String,String> couple;
		
		List<Pair<String,String>> couples = new LinkedList<>();
		for(String user1 : users) {
			i = users.indexOf(user1)+1;
			while (i < users.size()) {
				user2  = users.get(i);
				if (user1.compareTo(user2)<0)
					couple = new Pair<String,String>(user1,user2);
				else 
					couple = new Pair<String,String>(user2,user1);
				couples.add(couple);
				i++;
			}
				
		}
		return couples;
		
	}

}








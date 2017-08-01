package bigdata2.bigdata2;

import org.apache.giraph.graph.BasicComputation;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class CreateSubgraphs extends BasicComputation<
Text, VertexState, NullWritable, Text> {

	public static String cliqueFile;
	public static Map<Integer,IntArrayListWritable> megaCliques; // idsottografo -> lista cricche

	@Override
	public void compute( Vertex<Text, VertexState, NullWritable> vertex,
			Iterable<Text> messages) throws IOException {

		if (getSuperstep() == 0) {
			// invio un messaggio agli altri vertici per dire di duplicare gli archi
			// non posso duplicarli direttamente, ognuno si crea i suoi archi
			VertexState state = new VertexState();
			vertex.setValue(state);
			// duplico gli archi, perche' in input ho un grafo diretto 
			for (Edge<Text, NullWritable> edge : vertex.getEdges()) {
				Text messaggio = new Text(vertex.getId().toString());
//				messaggio.add(vertex.getId().toString());
				this.sendMessage(edge.getTargetVertexId(), messaggio);
			}
		}
		
		if (getSuperstep() == 1) {
			// duplico gli archi che mi sono arrivati
			for(Text message : messages){
					this.addEdgeRequest(vertex.getId(), EdgeFactory.create(new Text(message.toString())));
			}
		}
		
		if (getSuperstep() == 2) {
			/* imposto le mie cricche e le cricche che devo amministrare come mio stato,
			 * invio ai miei vicini le cricche a cui appartengo.
			 */
			setVertexCliques(vertex.getId(),vertex.getValue());
			if (!vertex.getValue().getMyCliques().isEmpty()) {
				for (Edge<Text, NullWritable> edge : vertex.getEdges()){
					sendMessage(edge.getTargetVertexId(),new Text(vertex.getValue().getMyCliques().toString()));
				}
			}
		}
		
		if (getSuperstep() == 3) {

			/* per ogni cricca C di cui sono l'admin, creo una lista di tutte le cricche raggiungibili,
			 * data dall'unione delle liste che mi sono arrivate come messaggio 
			 * che pero' hanno al loro interno la cricca C.
			 */
			ArrayList<IntArrayListWritable> megaCliques = new ArrayList<IntArrayListWritable>();
			for(IntWritable clique : vertex.getValue().getCliquesAdmin()){
//				System.out.println(vertex.getId()+" Sono admin di: "+clique);
				IntArrayListWritable megaClique = getUnionCliqueList(messages,clique,vertex.getValue().getMyCliques());
				megaCliques.add(megaClique);
			}
			// ora le rendo globali tramite un punto di sincronizzazione
			// l'aggregatore prende delle mappe (perche' non puo' prendere delle liste) e ne creo una
			MapWritable map = new MapWritable();
			int i = 0;
			for (IntArrayListWritable mc : megaCliques) {
				map.put(new IntWritable(i), new Text(mc.toString()));
				i++;
			}
			aggregate("MegaCliquesAgg",map );
		}
		
		if (getSuperstep() == 4) {
			// creo i nodi copia che faranno parte dei sottografi
			List<Integer> mySubgraphs = belongToSubgraphs(vertex.getValue());
			vertex.getValue().setMySubgraphs(mySubgraphs);
			if(!mySubgraphs.isEmpty()) {
				for(Integer subgraphId : mySubgraphs) {
					// qui va gestito meglio l'id TODO
					String fakeVertexId = vertex.getId().toString()+"-"+subgraphId.toString();
					Text text = new Text(fakeVertexId);
					vertex.getValue().addFakeVertex(text);
					addVertexRequest(text, null);
				}

				// invio ai miei vicini i nodi copia che ho creato
				for (Edge<Text, NullWritable> edge : vertex.getEdges())
					sendMessage(edge.getTargetVertexId(), new Text(vertex.getValue().getMyFakeVertices().toString()));

			}
		}
		
		if (getSuperstep() == 5) {
			// ho ricevuto come messaggio i nodi copia e ho inviato i miei, li collego
			
			if (vertex.getValue()!=null) {
				// mappo i vertici che mi sono arrivati per ogni sottografo
				Map<Integer,List<Text>> subgraphsToVertex = new HashMap<Integer,List<Text>>();
				for(Integer subgraph :vertex.getValue().getMySubgraphs())
					subgraphsToVertex.put(subgraph, new LinkedList<Text>());
				for(Text message : messages){
					String s = message.toString();
					s = s.substring(1, s.length()-1);
					s = s.replaceAll("\\s+","");
					List<String> items = Arrays.asList(s.split("\\s*,\\s*"));
					for(String vert : items) {
						String[] splitted = vert.split("-"); //TODO
						Integer subgraphId = Integer.parseInt(splitted[1]);
						if(subgraphsToVertex.containsKey(subgraphId))
							subgraphsToVertex.get(subgraphId).add(new Text(vert.toString()));
					}
				}
				// ora creo gli archi e invio i messaggi
				for(Integer subgraph : subgraphsToVertex.keySet()) {
					String myFakeVertex = vertex.getId().toString()+"-"+subgraph.toString();//TODO
					Text text = new Text(myFakeVertex);
					for (Text dest: subgraphsToVertex.get(subgraph)) {
						Text src = new Text(text);
						this.addEdgeRequest(src, EdgeFactory.create(new Text(dest.toString())));
					}
				}
				this.removeVertexRequest(vertex.getId());		
			}

		}

		if (getSuperstep() == 6) {
			vertex.voteToHalt();
		}


	}

	private List<Integer> belongToSubgraphs(VertexState state) {
		// calcolo i sottografi di cui il vertice fara' parte
		List<Integer> subgraphs = new LinkedList<Integer>();
		for(Integer subgraphId : megaCliques.keySet()) {
			Collection<?> intersection = CollectionUtils.
					intersection(state.getMyCliques(), megaCliques.get(subgraphId));
			if (!intersection.isEmpty())
				subgraphs.add(subgraphId);
		}
		return subgraphs;

	}

	private IntArrayListWritable getUnionCliqueList(Iterable<Text> messages,
			IntWritable myCliqueAdmin,IntArrayListWritable myCliques){
		/* unisco in un'unica lista le cricche che mi sono arrivate come messaggi dai miei vicini
		 * (che pero' contengono la cricca di cui sono admin)
		 * e le mie cricche
		 */
		IntArrayListWritable unionClique = new IntArrayListWritable();
		boolean found;
		for(Text message : messages) {
			found = false;
			String s = message.toString();
			s = s.substring(1, s.length()-1);
			s = s.replaceAll("\\s+","");
			List<String> items = Arrays.asList(s.split("\\s*,\\s*"));
			for(String c : items)
				if (Integer.parseInt(c)==myCliqueAdmin.get()) {
					found = true;
					break;
				}
			if (found) {
				unionClique = union2(unionClique,items);
			}

		}
		unionClique = union(unionClique,myCliques);
		return unionClique;
	}

	private void setVertexCliques(Text id,VertexState state) throws IOException {
		/* passo 0, setto come stato del vertice le cricche di appartenenza e le cricche 
		 * di cui e' admin e dovra' fare dei conti nei passi successivi.
		 */
		
		IntArrayListWritable vertexCliques = new IntArrayListWritable();
		IntArrayListWritable cliquesAdmin = new IntArrayListWritable();
		List<String> lines = FileUtils.readLines(new File(cliqueFile));

		Text vertexId = id;
		int cliqueId = 1;
		for(String line : lines){
			String[] splitted = line.split(",");
			List<Text> vertices = arrayStringToListText(splitted);			
			// splitted indica i nodi di una cricca
			if(vertices.contains(vertexId)) {
				vertexCliques.add(new IntWritable(cliqueId));
				Long prova = Long.valueOf(vertexId.toString()).longValue();
				// qui si genera un numero casuale ma uguale per tutti i vertici TODO
				if(Long.valueOf(vertices.get(1003%vertices.size()).toString()).longValue()==prova)
					cliquesAdmin.add(new IntWritable(cliqueId));
			}

			cliqueId++;
		}

		state.setMyCliques(vertexCliques);
		state.setCliquesAdmin(cliquesAdmin);
	}

	private List<Text> arrayStringToListText(String[] strings) {
		// converte un'array di stringhe contenenti long in una lista di Long
		List<Text> longList= new ArrayList<Text>(strings.length);
		for(String str:strings){
			Text t = new Text(str);
			longList.add(t);
		}
		return longList;
	}


	private IntArrayListWritable union(List<IntWritable> list1, List<IntWritable> list2) {
		// unione insiemistica tra due IntArrayListWritable (rimuovendo i duplicati)
		Set<IntWritable> set = new HashSet<IntWritable>();
		set.addAll(list1);
		set.addAll(list2);
		IntArrayListWritable result = new IntArrayListWritable();
		result.addAll(set);
		return result;
	}

	private IntArrayListWritable union2(List<IntWritable> list1, List<String> list2) {
		// unione insiemistica tra due IntArrayListWritable (rimuovendo i duplicati)
		Set<IntWritable> set = new HashSet<IntWritable>();
		set.addAll(list1);
		Set<IntWritable> setB = new HashSet<IntWritable>();
		for(String y : list2){
			IntWritable value = new IntWritable(Integer.parseInt(y));
			setB.add(value);
		}
		set.addAll(setB);
		IntArrayListWritable result = new IntArrayListWritable();
		result.addAll(set);
		return result;
	}
}















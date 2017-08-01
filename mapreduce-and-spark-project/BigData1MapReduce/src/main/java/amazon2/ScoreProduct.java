package amazon2;


public class ScoreProduct implements Comparable<ScoreProduct>{

	private int score;
	private String id;
	
	public ScoreProduct(String id,int score) {
		this.score = score;
		this.id = id;
	}

	public int getScore() {
		return score;
	}

	public void setScore(int score) {
		this.score = score;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	@Override
	public int compareTo(ScoreProduct o) {
		// gestisco i duplicati(in base all'id) personalmente nel reducer, quindi qui non ritorno mai 0
		if (this.score > o.score)
			return 1;
		else return -1;
	}	

}

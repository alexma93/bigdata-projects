package amazon1;

public class Product {

	private double score;
	private String idProd;
	
	public Product(String idProd,double score) {
		this.idProd = idProd;
		this.score = score;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

	public String getIdProd() {
		return idProd;
	}

	public void setIdProd(String idProd) {
		this.idProd = idProd;
	}
	
}






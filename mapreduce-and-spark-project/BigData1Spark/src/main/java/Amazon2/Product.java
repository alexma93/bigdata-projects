package Amazon2;

import java.io.Serializable;

public class Product implements Comparable<Product>{
	
	private static final long serialVersionUID = 1;

	private Double score;
	private String idProd;
	
	public Product(String idProd,Double score) {
		this.idProd = idProd;
		this.score = score;
	}
	

	public Double getScore() {
		return score;
	}

	public void setScore(Double score) {
		this.score = score;
	}

	public String getIdProd() {
		return idProd;
	}

	public void setIdProd(String idProd) {
		this.idProd = idProd;
	}


	@Override
	public int compareTo(Product o) {
		return (int) (this.score-o.getScore());
	}

}






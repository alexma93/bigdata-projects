package Amazon1;

import java.io.Serializable;

public class Product implements Serializable{
	
	private static final long serialVersionUID = 1;

	private String month;
	private String idProd;
	
	public Product(String idProd,String month) {
		this.idProd = idProd;
		this.month = month;
	}
	

	public String getMonth() {
		return month;
	}

	public void setMonth(String month) {
		this.month = month;
	}

	public String getIdProd() {
		return idProd;
	}

	public void setIdProd(String idProd) {
		this.idProd = idProd;
	}


}






package amazon3;

public class Pair<F, S> {

    private  F first;
    private  S second;

    public Pair(F element0, S element1) {
        this.first = element0;
        this.second = element1;
    }

    public F getFirst() {
        return first;
    }

    public S getSecond() {
        return second;
    }

	public void setFirst(F first) {
		this.first = first;
	}

	public void setSecond(S second) {
		this.second = second;
	}

}

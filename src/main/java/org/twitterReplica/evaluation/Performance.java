package org.twitterReplica.evaluation;

public class Performance {

	private int truePositives;
	private int falsePositives;
	private int falseNegatives;
	private int trueNegatives;
	
	public Performance() {
		super();
		this.truePositives = 0;
		this.falsePositives = 0;
		this.falseNegatives = 0;
		this.trueNegatives = 0;
	}

	public void addTruePositive() {
		this.truePositives++;
	}
	
	public void addFalsePositive() {
		this.falsePositives++;
	}

	public void addFalseNegative() {
		this.falseNegatives++;
	}
	
	public void addTrueNegative() {
		this.trueNegatives++;
	}
	
	// Get measures

	public int getTruePositives() {
		return truePositives;
	}
	
	public int getFalsePositives() {
		return falsePositives;
	}

	public int getFalseNegatives() {
		return falseNegatives;
	}
	public int getTrueNegatives() {
		return trueNegatives;
	}
	
	/*
	 * 	Computes the precision
	 * 	@return Precision for the given class
	 */
	public double getPrecision() {
		double num = this.truePositives;
		double den = this.truePositives + this.falsePositives;
		return den == 0 ? 0 : (num / den);
	}

	/*
	 * 	Computes the recall
	 * 	@return Recall for the given class
	 */
	public double getRecall() {
		double num = this.truePositives;
		double den = this.truePositives + this.falseNegatives;
		return den == 0 ? 0 : (num / den);
	}
	
	public void displayResults() {
		System.out.println("\nShowing total results: ");
		System.out.println("-------------------------------------");
		System.out.println("Precision: " + this.getPrecision());
		System.out.println("Recall: " + this.getRecall());
		System.out.println();
	}
	
}

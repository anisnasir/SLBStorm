package storm.starter;


public class Seed {
	public int SEEDS[];
	int currentPrime = 2;
	
	Seed(int workers) { 
		SEEDS = new int[workers];
		for (int i = 0 ; i< workers; i++) {
			SEEDS[i] = currentPrime;
			currentPrime = getNextPrime(currentPrime);
		}	
	}
	
	private int getNextPrime(int x) {
		int num = x+1;
		while(!isPrime(num)) {
			num++;
		}
		return num;
	}

	private boolean isPrime(int num) {
		for(int i= 2; i<num;i++) {
			if (num%i == 0) {
				return false;
			}
		}
		return true;
	}
	
}

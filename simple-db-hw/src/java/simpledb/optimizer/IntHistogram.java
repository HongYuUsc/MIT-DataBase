package simpledb.optimizer;

import java.nio.file.spi.FileSystemProvider;
import java.util.concurrent.ConcurrentHashMap;

import net.sf.antcontrib.perf.AntPerformanceListener;
import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

	private ConcurrentHashMap<Integer, Integer> Histogram;
	private int interval;
	private int min;
	private int max;
	private int ntups;
	
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
    	interval = (max - min)/buckets + 1;
    	Histogram = new ConcurrentHashMap<Integer, Integer>();
    	this.min = min;
    	this.max = max;
    	for(int i=0;i<buckets;i++) {
    		Histogram.put(min+i*interval, 0);
    	}
    	ntups = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
    	int bucketnum = (v-min)/interval;
    	int key = min + bucketnum*interval;
    	Integer val = Histogram.get(key);
    	Histogram.put(key, val+1);
    	ntups++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	String opString = op.toString();
    	if(v < min && (op.toString() == ">" || op.toString() == ">=")) {
    		v = min;
    		opString = ">=";
    	}else if(v > max && (op.toString() == "<" || op.toString() == "<=")) {
    		v = max;
    		opString = "<=";
    	}
    	int bucketnum = (v-min)/interval;
    	int key = min + bucketnum*interval;
    	Integer height = Histogram.get(key);
    	double res = 0.0;
    	if(height == null) {
    		height = 0;
    	}
    	if(opString == "=") {
    		res = (double)(height/interval)/(double)(ntups);
    	}else if(opString == ">" || opString == ">=") {
    		if(opString == ">") {
    			res += ((double)(key + interval - 1 - v)/(double)(interval))*((double)height/(double)ntups);
    		}else if(opString == ">=") {
    			res += ((double)(key + interval - v)/(double)(interval))*((double)height/(double)ntups);
    		}
    		key += interval;
    		for(;key<=max;key+=interval) {
    			res += ((double)Histogram.get(key)/(double)(interval))/(double)ntups;
    		}
    	}else if(opString == "<" || opString == "<=") {
    		if(opString == "<") {
    			res += ((double)(v-key)/(double)(interval))*((double)height/(double)ntups);
    		}else if(opString == "<=") {
    			res += ((double)(v-key+1)/(double)(interval))*((double)height/(double)ntups);
    		}
    		key -= interval;
    		for(;key>=min;key-=interval) {
    			res += ((double)Histogram.get(key)/(double)(interval))/(double)ntups;
    		}
    	}else if(opString == "<>") {
    		res = 1 - (double)(height/interval)/(double)(ntups);
    	}
    	
    	// some code goes here
        return res;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
    	String res = new String();
    	for(Integer key:Histogram.keySet()) {
    		res = res + "key=" + key + "height=" + Histogram.get(key) + "\n";
    	}
        return res;
    }
}

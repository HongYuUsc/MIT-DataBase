package simpledb.execution;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    private final int groupField;
    private final int aggregateField;
    private final Type fieldType;
    private final Op operatorOp;
    private TreeMap<Object, ArrayList<Integer>> tuples;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	groupField = gbfield;
    	aggregateField = afield;
    	fieldType = gbfieldtype;
    	operatorOp = what;
    	tuples = new TreeMap<Object, ArrayList<Integer>>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	if(groupField == NO_GROUPING) {
    		int aggrval = ((IntField)tup.getField(aggregateField)).getValue();
    		if(tuples.get(NO_GROUPING) == null) {
        		ArrayList<Integer> aggreList = new ArrayList<Integer>();
        		aggreList.add(aggrval);
        		tuples.put(NO_GROUPING, aggreList);
        	}else {
        		tuples.get(NO_GROUPING).add(aggrval);
        	}
    		return;
    	}else if(fieldType == Type.INT_TYPE) {
    		int groupval = ((IntField)tup.getField(groupField)).getValue();
        	int aggrval = ((IntField)tup.getField(aggregateField)).getValue();
        	if(tuples.get(groupval) == null) {
        		ArrayList<Integer> aggreList = new ArrayList<Integer>();
        		aggreList.add(aggrval);
        		tuples.put(groupval, aggreList);
        	}else {
        		tuples.get(groupval).add(aggrval);
        	}
    	}else if(fieldType == Type.STRING_TYPE) {
    		String groupval = ((StringField)tup.getField(groupField)).getValue();
        	int aggrval = ((IntField)tup.getField(aggregateField)).getValue();
        	if(tuples.get(groupval) == null) {
        		ArrayList<Integer> aggreList = new ArrayList<Integer>();
        		aggreList.add(aggrval);
        		tuples.put(groupval, aggreList);
        	}else {
        		tuples.get(groupval).add(aggrval);
        	}
    	}
    }
    
   
    public class IntegerAggOpIterator implements OpIterator{
    	/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private ArrayList<Tuple> resArrayList;
    	private Iterator<Tuple> iter;
    	private TupleDesc tDesc;
    	
    	public IntegerAggOpIterator() {
    		iter = null;
    		resArrayList = new ArrayList<Tuple>();
    		if(groupField == NO_GROUPING) {
        		for(Object key:tuples.keySet()) {
        			ArrayList<Integer> arrayList = tuples.get(key);
        			tDesc = new TupleDesc(new Type[] {Type.INT_TYPE});
        			Tuple tuple = new Tuple(tDesc);
        			tuple.setField(0, new IntField(aggregation(arrayList)));
        			resArrayList.add(tuple);
        		}
        		return;
        	}else if(fieldType == Type.INT_TYPE) {
        		for(Object key:tuples.keySet()) {
        			ArrayList<Integer> arrayList = tuples.get(key);
        			tDesc = new TupleDesc(new Type[] {Type.INT_TYPE,Type.INT_TYPE});
        			Tuple tuple = new Tuple(tDesc);
        			tuple.setField(0, new IntField(((Integer)key).intValue()));
        			tuple.setField(1, new IntField(aggregation(arrayList)));
        			resArrayList.add(tuple);
        		}
        	}else if(fieldType == Type.STRING_TYPE) {
        		for(Object key:tuples.keySet()) {
        			ArrayList<Integer> arrayList = tuples.get(key);
        			tDesc = new TupleDesc(new Type[] {Type.STRING_TYPE,Type.INT_TYPE});
        			Tuple tuple = new Tuple(tDesc);
        			tuple.setField(0, new StringField((String)key,100));
        			tuple.setField(1, new IntField(aggregation(arrayList)));
        			resArrayList.add(tuple);
        		}
        	}
    	}
    	
    	 /**
         *  Aggregate Operator
         */
        private int aggregation(ArrayList<Integer> arrayList) {
        	int ans = 0;
    		switch (operatorOp) {
        		case MIN:
        			ans = arrayList.get(0);
        			for(int val:arrayList) {
        				ans = Math.min(val, ans);
        			}
    				return ans;
    			case MAX:
    				ans = arrayList.get(0);
        			for(int val:arrayList) {
        				ans = Math.max(val, ans);
        			}
    				return ans;
    			case SUM:
    				for(int val:arrayList) {
        				ans += val;
        			}
    				return ans;
    			case AVG:
    				for(int val:arrayList) {
        				ans += val;
        			}
    				return ans/arrayList.size();
    			case COUNT:
    				return arrayList.size();
    			default:
    				break;
    		}
        	
    		return ans;
        }
        

		@Override
		public void open() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			iter = resArrayList.iterator();
		}

		@Override
		public boolean hasNext() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			return iter.hasNext();
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
			// TODO Auto-generated method stub
			return iter.next();
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			close();
			open();
		}

		@Override
		public TupleDesc getTupleDesc() {
			// TODO Auto-generated method stub
			return tDesc;
		}

		@Override
		public void close() {
			// TODO Auto-generated method stub
			iter = null;
		}
    	
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new IntegerAggOpIterator();
    }

}

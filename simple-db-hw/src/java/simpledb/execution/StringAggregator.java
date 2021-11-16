package simpledb.execution;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Aggregator.Op;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int groupField;
    private final int aggregateField;
    private final Op operatorOp;
    private final Type fieldtype;
    private TreeMap<Object, ArrayList<String>> tuples;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	groupField = gbfield;
    	aggregateField = afield;
    	operatorOp = what;
    	fieldtype = gbfieldtype;
    	tuples = new TreeMap<Object, ArrayList<String>>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	if(groupField == NO_GROUPING) {
    		String aggrval = ((StringField)tup.getField(aggregateField)).getValue();
    		if(tuples.get(NO_GROUPING) == null) {
        		ArrayList<String> aggreList = new ArrayList<String>();
        		aggreList.add(aggrval);
        		tuples.put(NO_GROUPING, aggreList);
        	}else {
        		tuples.get(NO_GROUPING).add(aggrval);
        	}
    		return;
    	}else if(fieldtype == Type.INT_TYPE) {
    		int groupval = ((IntField)tup.getField(groupField)).getValue();
        	String aggrval = ((StringField)tup.getField(aggregateField)).getValue();
        	if(tuples.get(groupval) == null) {
        		ArrayList<String> aggreList = new ArrayList<String>();
        		aggreList.add(aggrval);
        		tuples.put(groupval, aggreList);
        	}else {
        		tuples.get(groupval).add(aggrval);
        	}
    	}else if(fieldtype == Type.STRING_TYPE) {
    		String groupval = ((StringField)tup.getField(groupField)).getValue();
    		String aggrval = ((StringField)tup.getField(aggregateField)).getValue();
        	if(tuples.get(groupval) == null) {
        		ArrayList<String> aggreList = new ArrayList<String>();
        		aggreList.add(aggrval);
        		tuples.put(groupval, aggreList);
        	}else {
        		tuples.get(groupval).add(aggrval);
        	}
    	}
    }
    
    public class StringAggOpIterator implements OpIterator{
    	/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private ArrayList<Tuple> resArrayList;
    	private Iterator<Tuple> iter;
    	private TupleDesc tDesc;
    	
    	public StringAggOpIterator() {
    		iter = null;
    		resArrayList = new ArrayList<Tuple>();
    		if(groupField == NO_GROUPING) {
        		for(Object key:tuples.keySet()) {
        			ArrayList<String> arrayList = tuples.get(key);
        			tDesc = new TupleDesc(new Type[] {Type.INT_TYPE});
        			Tuple tuple = new Tuple(tDesc);
        			tuple.setField(0, new IntField(arrayList.size()));
        			resArrayList.add(tuple);
        		}
        		return;
        	}else if(fieldtype == Type.INT_TYPE) {
        		for(Object key:tuples.keySet()) {
        			ArrayList<String> arrayList = tuples.get(key);
        			tDesc = new TupleDesc(new Type[] {Type.INT_TYPE,Type.INT_TYPE});
        			Tuple tuple = new Tuple(tDesc);
        			tuple.setField(0, new IntField(((Integer)key).intValue()));
        			tuple.setField(1, new IntField(arrayList.size()));
        			resArrayList.add(tuple);
        		}
        	}else if(fieldtype == Type.STRING_TYPE) {
        		for(Object key:tuples.keySet()) {
        			ArrayList<String> arrayList = tuples.get(key);
        			tDesc = new TupleDesc(new Type[] {Type.STRING_TYPE,Type.INT_TYPE});
        			Tuple tuple = new Tuple(tDesc);
        			tuple.setField(0, new StringField((String)key,100));
        			tuple.setField(1, new IntField(arrayList.size()));
        			resArrayList.add(tuple);
        		}
        	}
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
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new StringAggOpIterator();
    }

}

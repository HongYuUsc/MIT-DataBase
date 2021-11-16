package simpledb.execution;

import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    private final Predicate predicate;
    private OpIterator opIterator;
    
    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // some code goes here
    	predicate = p;
    	opIterator = child;
    }

    public Predicate getPredicate() {
        // some code goes here
        return predicate;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return opIterator.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
    	super.open();
    	opIterator.open();
    }

    public void close() {
        // some code goes here
    	super.close();
    	opIterator.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	opIterator.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
    	while(opIterator.hasNext()) {
    		Tuple tuple = opIterator.next();
    		if(predicate.filter(tuple)) {
    			return tuple;
    		}
    	}
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
    	OpIterator[] operators = new OpIterator[1];
    	operators[0] = opIterator;
        return operators;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    	opIterator = children[0];
    }

}

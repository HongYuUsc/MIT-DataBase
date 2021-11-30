package simpledb.execution;

import java.io.IOException;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tId;
    private OpIterator childIterator;
    private int tableId;
    private int count;
    private TupleDesc tDesc;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
    	tId = t;
    	childIterator = child;
        this.tableId = tableId;
        count = 0;
        tDesc = new TupleDesc(new Type[] {Type.INT_TYPE});
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	super.open();
    	childIterator.open();
    }

    public void close() {
        // some code goes here
    	super.close();
    	childIterator.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	childIterator.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if(!childIterator.hasNext()) {
    		return null;
    	}
    	Tuple tuple = null;
    	while(childIterator.hasNext()) {
    		tuple = childIterator.next();
    		try {
				Database.getBufferPool().insertTuple(tId, tableId, tuple);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		count++;
    	}
    	Tuple restuple = new Tuple(tDesc);
    	restuple.setField(0, new IntField(count));
        return restuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
    	OpIterator[] opIterators = new OpIterator[1];
    	opIterators[0] = childIterator;
        return opIterators;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    	childIterator = children[0];
    }
}

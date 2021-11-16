package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tId;
    private OpIterator childIterator;
    private int tableId;
    private int count;
    private TupleDesc tDesc;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
    	tId = t;
    	childIterator = child;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	Tuple tuple = null;
    	while(childIterator.hasNext()) {
    		tuple = childIterator.next();
    		try {
				Database.getBufferPool().deleteTuple(tId, tuple);
			} catch (DbException | IOException | TransactionAbortedException e) {
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
        return null;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    }

}

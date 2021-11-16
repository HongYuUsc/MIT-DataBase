package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator childOpIterator;
    private int aggregateField;
    private int groupField;
    private Aggregator.Op aggregateOperator;
    private Aggregator aggregator;
    
    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
    	aggregateField = afield;
    	groupField = gfield;
    	aggregateOperator = aop;
    	Type gfieldType = null;
    	if(gfield >= 0) {
    		gfieldType = child.getTupleDesc().getFieldType(gfield);
    	}
    	if(child.getTupleDesc().getFieldType(afield).equals(Type.INT_TYPE)) {
    		aggregator = new IntegerAggregator(gfield, gfieldType, afield, aop);
    	}else if(child.getTupleDesc().getFieldType(afield).equals(Type.STRING_TYPE)) {
    		aggregator = new StringAggregator(gfield, gfieldType, afield, aop);
    	}
    	try {
    		child.open();
			while(child.hasNext()) {
				aggregator.mergeTupleIntoGroup(child.next());
			}
		} catch (DbException | TransactionAbortedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	childOpIterator = aggregator.iterator();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        return groupField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        if(groupField == -1) {
        	return null;
        }
        return getTupleDesc().getFieldName(groupField);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return aggregateField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        return getTupleDesc().getFieldName(aggregateField);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return aggregateOperator;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
    	super.open();
    	childOpIterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if(childOpIterator.hasNext()) {
    		return childOpIterator.next();
    	}
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	childOpIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return childOpIterator.getTupleDesc();
    }

    public void close() {
        // some code goes here
    	super.close();
    	childOpIterator.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
    	OpIterator[] opIterators = new OpIterator[1];
    	opIterators[0] = childOpIterator;
        return opIterators;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    	childOpIterator = children[0];
    }

}

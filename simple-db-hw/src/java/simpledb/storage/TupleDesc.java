package simpledb.storage;

import simpledb.common.Type;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
        
        public boolean equals(Object obj) {
            if (obj != null && obj instanceof TDItem) {
                return fieldType.equals(((TDItem) obj).fieldType);
            }
            return false;
        }
    }
    
    private ArrayList<TDItem> fieldsArrayList;

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return fieldsArrayList.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
    	assertEquals(typeAr.length, fieldAr.length);
    	int length = typeAr.length;
    	fieldsArrayList = new ArrayList<TDItem>(length);
    	for(int i=0;i<length;i++) {
    		TDItem td = new TDItem(typeAr[i], fieldAr[i]);
    		fieldsArrayList.add(td);
    	}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
    	int length = typeAr.length;
    	fieldsArrayList = new ArrayList<TDItem>(length);
    	for(int i=0;i<length;i++) {
    		TDItem td = new TDItem(typeAr[i],"");
    		fieldsArrayList.add(td);
    	}
    }

	/**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return fieldsArrayList.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
    	if(i >= fieldsArrayList.size() || i < 0) {
    		throw new NoSuchElementException();
    	}
        return fieldsArrayList.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
    	if(i >= fieldsArrayList.size() || i < 0) {
    		throw new NoSuchElementException();
    	}
        return fieldsArrayList.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
    	for(int i=0;i<fieldsArrayList.size();i++) {
    		if(fieldsArrayList.get(i).fieldName.equals(name)) {
    			return i;
    		}
    	}
    	throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
    	int res = 0;
        for(int i=0;i<fieldsArrayList.size();i++) {
        	res += fieldsArrayList.get(i).fieldType.getLen();
        }
        return res;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        int size1 = td1.numFields();
        int size2 = td2.numFields();
        int size = size1 + size2;
        Type[] types = new Type[size];
        String[] fieldnames = new String[size];
        for(int i=0;i<size1;i++) {
        	types[i] = td1.getFieldType(i);
        	fieldnames[i]= td1.getFieldName(i); 
        }
        for(int i=size1;i<size;i++) {
        	types[i] = td2.getFieldType(i-size1);
        	fieldnames[i]= td2.getFieldName(i-size1);
        }
        return new TupleDesc(types, fieldnames);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
    	return o == null ? fieldsArrayList == null :
            (o instanceof TupleDesc && fieldsArrayList.equals(((TupleDesc) o).fieldsArrayList));
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
    	String res = new String("");
        for(int i=0;i<fieldsArrayList.size();i++) {
        	res += fieldsArrayList.get(i).toString();
        }
        return res;
    }
}

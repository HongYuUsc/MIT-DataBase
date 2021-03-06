package simpledb.storage;

import simpledb.common.Catalog;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

import javax.print.attribute.standard.PagesPerMinute;
import javax.swing.plaf.basic.BasicInternalFrameTitlePane.SystemMenuBar;

import net.sf.antcontrib.logic.IfTask;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

	final File hFile;
	final TupleDesc tDesc;
	
	
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	hFile = f;
    	tDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return hFile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return hFile.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
		// some code goes here
    	int pagesize = BufferPool.getPageSize();
    	RandomAccessFile rf;
    	Page p = null;
    	byte[] data = new byte[pagesize];
		try {
			rf = new RandomAccessFile(hFile, "rw");
			rf.seek(pagesize * pid.getPageNumber());
			rf.read(data, 0, pagesize);
			p = new HeapPage((HeapPageId)pid, data);
		} catch (IOException e) {
			e.printStackTrace();
		}
        return p;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    	PageId pid = page.getId();
    	int pgno = pid.getPageNumber();
    	int pgsize = Database.getBufferPool().getPageSize();
    	byte[] data = page.getPageData();
    	RandomAccessFile out = new  RandomAccessFile(hFile, "rws");
    	out.skipBytes(pgno * pgsize);
    	out.write(data);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
    	RandomAccessFile rf;
    	int nums = 0;
    	try {
			rf = new RandomAccessFile(hFile, "rw");
			nums = (int) rf.length()/BufferPool.getPageSize();
		} catch (IOException e) {
			e.printStackTrace();
		}
    	return nums;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
    	int num = numPages();
    	ArrayList<Page> resArrayList = new ArrayList<Page>();
    	for(int i=0;i<num;i++) {
    		PageId pId = new HeapPageId(getId(),i);
    		HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pId, Permissions.READ_WRITE);
    		if(page.getNumEmptySlots() > 0) {
    			page.insertTuple(t);
    			resArrayList.add(page);
    			page.markDirty(true, tid);
    			return resArrayList;
    		}
    		//Database.getBufferPool().unsafeReleasePage(tid, pId);
    	}
    	
    	HeapPageId pid = new HeapPageId(getId(), num);
        HeapPage newpg = new HeapPage(pid, HeapPage.createEmptyPageData());
        if(newpg.getNumEmptySlots() > 0) {
    		newpg.insertTuple(t);
    		resArrayList.add(newpg);
    		writePage(newpg);
    		return resArrayList;
    	}
        
        throw new DbException("error on insertTuple: no Tuple can insert");
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
    	int num = numPages();
    	RecordId rId = t.getRecordId();
    	PageId pId = rId.getPageId();
    	HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pId, Permissions.READ_WRITE);
    	page.deleteTuple(t);
    	page.markDirty(true, tid);
    	ArrayList<Page> resArrayList = new ArrayList<Page>();
    	resArrayList.add(page);
        return resArrayList;
        // not necessary for lab1
    }
    
    private class HeapFileIterator implements DbFileIterator {

        private Integer pgCursor;
        private Iterator<Tuple> tupleIter;
        private final TransactionId transactionId;
        private final int tableId;
        private final int numPages;

        public HeapFileIterator(TransactionId tid) {
            this.pgCursor = null;
            this.tupleIter = null;
            this.transactionId = tid;
            this.tableId = getId();
            this.numPages = numPages();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            pgCursor = 0;
            tupleIter = getTupleIter(pgCursor);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            // < numpage - 1
            if (pgCursor != null) {
                while (pgCursor < numPages - 1) {
                    if (tupleIter.hasNext()) {
                        return true;
                    } else {
                        pgCursor += 1;
                        tupleIter = getTupleIter(pgCursor);
                    }
                }
                return tupleIter.hasNext();
            } else {
                return false;
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (hasNext())  {
                return tupleIter.next();
            }
            return null;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            pgCursor = null;
            tupleIter = null;
        }

        private Iterator<Tuple> getTupleIter(int pgNo)
                throws TransactionAbortedException, DbException {
            PageId pid = new HeapPageId(tableId, pgNo);
            return ((HeapPage)
                    Database
                            .getBufferPool()
                            .getPage(transactionId, pid, Permissions.READ_ONLY))
                    .iterator();
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

}


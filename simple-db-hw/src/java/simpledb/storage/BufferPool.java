package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.xml.catalog.Catalog;

import net.sf.antcontrib.logic.IfTask;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
	
	enum LockType {
		SLOCK,
		XLOCK 
	}
	
	public class LockManager{
		/* Locker is the implementation of SLock and XLock */
		private class Locker{
			private LockType type;
			private PageId pId;
			private TransactionId tId;
			
			public Locker(TransactionId tId,PageId pId, LockType type) {
				this.type = type;
				this.pId = pId;
				this.tId = tId;
			}
			
			public void SetLockType(LockType type) {
				this.type = type;
			}
			
			public LockType GetLockType() {
				return type;
			}
			
			public TransactionId GetTransactionId() {
				return tId;
			}
			
			public PageId GetPageId() {
				return pId;
			}
		}
		
		private ConcurrentHashMap<TransactionId, HashSet<Locker>> tid_lock;
		private ConcurrentHashMap<PageId, HashSet<TransactionId>> pid_tids;
		private ConcurrentHashMap<TransactionId, HashSet<TransactionId>> wait_graph;
		
		public LockManager() {
			tid_lock = new ConcurrentHashMap<TransactionId, HashSet<Locker>>();
			pid_tids = new ConcurrentHashMap<PageId, HashSet<TransactionId>>();
			wait_graph = new ConcurrentHashMap<TransactionId, HashSet<TransactionId>>();
		}
		
		public synchronized void acquireLock(TransactionId tId,PageId pId,LockType type) throws TransactionAbortedException {
			while(true) {
				HashSet<TransactionId> tIds = pid_tids.get(pId);
				boolean blocked = false;
				HashSet<TransactionId> blockList = new HashSet<TransactionId>();
				if(tIds!=null) {
					if(type == LockType.XLOCK) {
						if(isLocked(tId, pId)) {
							HashSet<Locker> lockers = tid_lock.get(tId);
							if(tIds.size() == 1) {
								/* Only one lock, upgrade the locktype to XLOCK */
								for(Locker locker:lockers) {
									if(locker.GetPageId() == pId) {
										locker.SetLockType(type);
									}
								}
								return;
							}else {
								Iterator<TransactionId> iterator = tIds.iterator();
								while(iterator.hasNext()) {
									TransactionId blockId = iterator.next();
									if(blockId != tId) {
										blockList.add(tId);
									}
								}
								blocked = true;
							}
						}else if(tIds.size() > 0) {
							Iterator<TransactionId> iterator = tIds.iterator();
							while(iterator.hasNext()) {
								blockList.add(iterator.next());
							}
							blocked = true;
						}
					}else if(type == LockType.SLOCK) {
						if(isLocked(tId, pId)) {
							return;
						}else {
							for(TransactionId tid:tIds) {
								HashSet<Locker> lockers = tid_lock.get(tid);
								for(Locker locker:lockers) {
									if(locker.GetPageId() == pId && locker.GetLockType() == LockType.XLOCK) {
										blocked = true;
										blockList.add(tid);
									}
								}
							}
						}
					}
					if(blocked) {
						/*check whether there is a deadlock*/
						wait_graph.put(tId, blockList);
						if(isdeadlock(tId, tId)) {
							wait_graph.remove(tId);
							throw new TransactionAbortedException();
						}
						sleepForRandomTime();
						continue;
					}
				}
				wait_graph.remove(tId);
				
				if(tIds == null) {
					tIds = new HashSet<TransactionId>();
				}
				tIds.add(tId);
				pid_tids.put(pId, tIds);
				Locker locker = new Locker(tId,pId, type);
				HashSet<Locker> lockers = tid_lock.get(tId);
				if(lockers == null) {
					lockers = new HashSet<Locker>();
				}
				lockers.add(locker);
				tid_lock.put(tId, lockers);
				break;
			}
		}
		
		public synchronized void releaseAllLocks(TransactionId tId) {
			ArrayList<PageId> pIds = lockManager.getLockedPageIds(tId);
			if(pIds == null) {
				return;
			}
			for(PageId pid:pIds) {
				releaseLock(tId, pid);
			}
		}
		
		public synchronized ArrayList<PageId> getLockedPageIds(TransactionId tId) {
			HashSet<Locker> lockers = tid_lock.get(tId);
			if(lockers == null) {
				return null;
			}
			ArrayList<PageId> pIds = new ArrayList<PageId>();
			Iterator<Locker> iterator = lockers.iterator();
			while(iterator.hasNext()) {
				pIds.add(iterator.next().pId);
			}
			return pIds;
		}
		
		public synchronized void releaseLock(TransactionId tId,PageId pId) {
			HashSet<TransactionId> htTransactionIds = pid_tids.get(pId);
			if(htTransactionIds == null) {
				return;
			}
			htTransactionIds.remove(tId);
			if(htTransactionIds.size() == 0) {
				pid_tids.remove(pId);
			}
			
			HashSet<Locker> lockers = tid_lock.get(tId);
			Iterator<Locker> iterator = lockers.iterator();
			while(iterator.hasNext()) {
				Locker locker = iterator.next();
				if(locker.GetPageId() == pId) {
					iterator.remove();
				}
			}
			if(lockers.size() == 0) {
				tid_lock.remove(tId);
			}
		}
		
		public synchronized boolean isLocked(TransactionId tId,PageId pId) {
			HashSet<TransactionId> htTransactionIds = pid_tids.get(pId);
			return htTransactionIds.contains(tId);
		}
		
		public synchronized boolean isPageLocked(PageId pId) {
			HashSet<TransactionId> htTransactionIds = pid_tids.get(pId);
			return htTransactionIds != null && htTransactionIds.size()>0;
		}
		
		private synchronized boolean isdeadlock(TransactionId targeId, TransactionId tId) {
			HashSet<TransactionId> tIds = wait_graph.get(tId);
			if(tIds == null || tIds.size() == 0) {
				return false;
			}
			Iterator<TransactionId> blockIterator = tIds.iterator();
			while(blockIterator.hasNext()) {
				TransactionId blockId = blockIterator.next();
				if(blockId == targeId || isdeadlock(targeId, blockId)) {
					return true;
				}
			}
			return false;
		}
		
		private void sleepForRandomTime() {
			Random random = new Random();
			try {
				wait(random.nextInt(500));
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    
    private final int max_page_nums;
    private final LinkedHashMap<PageId, Page> pageMap;
    private TransactionId tId;
    private LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
    	max_page_nums = numPages;
    	pageMap = new LinkedHashMap<PageId, Page>(numPages);
    	lockManager = new LockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
    	LockType lockType = (perm == Permissions.READ_ONLY ? LockType.SLOCK : LockType.XLOCK);
    	lockManager.acquireLock(tid, pid, lockType);
    	Page pg = pageMap.get(pid);
    	if(pg == null) {
    		if(pageMap.size() >= max_page_nums) {
        		evictPage();
        	}
    		pg = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
    		pageMap.put(pid, pg);
    	}
    	tId = tid;
    	return pg;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    	lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
    	transactionComplete(tid,true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.isLocked(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
    	if(commit) {
    		try {
				flushPages(tid);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}else {
    		ArrayList<PageId> pIds = lockManager.getLockedPageIds(tid);
    		for(PageId pid:pIds) {
    			HeapPage pg = (HeapPage)Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
        		pageMap.put(pid, pg);
    		}
    	}
    	lockManager.releaseAllLocks(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	ArrayList<Page> pages = (ArrayList<Page>)(Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t));
    	for(int i=0;i<pages.size();i++) {
    		((HeapPage)pages.get(i)).markDirty(true, tid);
    		pageMap.put(pages.get(i).getId(), pages.get(i));
    	}
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        RecordId rId = t.getRecordId();
        Page page = getPage(tid, rId.getPageId(), Permissions.READ_WRITE);
        ((HeapPage)page).markDirty(true, tid);
        ((HeapPage)page).deleteTuple(t);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        Iterator<Map.Entry<PageId, Page>> it = pageMap.entrySet().iterator();
        while(it.hasNext()) {
        	Map.Entry<PageId, Page> entry = (Map.Entry<PageId, Page>)it.next();
        	flushPage(entry.getKey());
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
    	pageMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
    	Page page = pageMap.get(pid);
    	if(page == null) {
    		return;
    	}
    	if(page.isDirty() != null) {
    		try {
				Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
			} catch (NoSuchElementException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    	page.markDirty(false, tId);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	ArrayList<PageId> pIds = lockManager.getLockedPageIds(tid);
    	if(pIds == null) {
    		return;
    	}
    	Iterator<PageId> iterator = pIds.iterator();
    	while(iterator.hasNext()) {
    		flushPage(iterator.next());
    	}
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
    	Iterator<Entry<PageId, Page>> iterator = pageMap.entrySet().iterator();
    	while(iterator.hasNext()) {
    		Page page = iterator.next().getValue();
    		if(page.isDirty() != null) {
    			continue;
    		}
    		iterator.remove();
    		return;
    	}
    	throw new DbException("all pages are dirty");
    }

}

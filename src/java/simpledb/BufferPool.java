package simpledb;

import java.io.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
    private ConcurrentHashMap<PageId,Page> findPage;
    private int numPages;

    public class Lock{
        TransactionId tid;
        int type;
        Lock(){}
        Lock(TransactionId tid,int type){
            this.tid=tid;this.type=type;
        }
    }
    private ConcurrentHashMap<PageId, ArrayList<Lock>> lockMap;
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        findPage=new ConcurrentHashMap<>(numPages);
        lockMap=new ConcurrentHashMap<>(numPages);
        this.numPages=numPages;
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
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        for(ConcurrentHashMap.Entry<PageId,Page> item:findPage.entrySet()){
            if(item.getKey().equals(pid)){
                ArrayList<Lock> locks=lockMap.get(pid);
                for(Lock lock:locks){
                    if(lock.tid.equals(tid))break;
                    if(lock.type==1||perm.permLevel==1){
                        try{
                            Thread.sleep(200);
                        }catch(Exception e){
                            e.printStackTrace();
                        }
                    }
                }
                locks.add(new Lock(tid,perm.permLevel));
                return item.getValue();
            }
        }
        while(findPage.size()>numPages)
            evictPage();
        DbFile dbfile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page p=dbfile.readPage(pid);
        findPage.put(pid,p);
        lockMap.put(pid,new ArrayList<>());
        lockMap.get(pid).add(new Lock(tid,perm.permLevel));
        return p;
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
    public void releasePage(TransactionId tid, PageId pid) {
        ArrayList<Lock> locks=lockMap.get(pid);
        for(Lock lock:locks){
            if(lock.tid.equals(tid)){
                locks.remove(lock);break;
            }
        }
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid,true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        ArrayList<Lock> locks=lockMap.get(p);
        for(Lock lock:locks){
            if(lock.tid.equals(tid)){
                return true;
            }
        }
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        if(commit){
            flushPages(tid);
        }
        else{
            for(Map.Entry<PageId,Page> entry:findPage.entrySet()){
                Page page=entry.getValue();
                PageId pid=entry.getKey();
                if(page.isDirty()!=null&&page.isDirty().equals(tid)){
                    int tableId=pid.getTableId();
                    DbFile dbFile=Database.getCatalog().getDatabaseFile(tableId);
                    Page p=dbFile.readPage(pid);
                    findPage.put(pid,p);
                }
            }
        }

        for(PageId pid:findPage.keySet()){
            if(holdsLock(tid,pid)){
                releasePage(tid,pid);
            }
        }
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
        DbFile file=Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page>pages= file.insertTuple(tid,t);
        for(Page page:pages){
            page.markDirty(true,tid);
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
        int tableId=t.getRecordId().getPageId().getTableId();
        DbFile file=Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page>pages= file.deleteTuple(tid,t);
        for(Page page:pages){
            page.markDirty(true,tid);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for(Page page:findPage.values()){
            if(page.isDirty()!=null)
                flushPage(page.getId());
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
        if(!findPage.containsKey(pid))
            return;
        findPage.remove(pid);
        lockMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        Page p=findPage.get(pid);
        Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(p);
        p.markDirty(false,null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        for(Map.Entry<PageId,Page> entry:findPage.entrySet()){
            Page page=entry.getValue();
            PageId pid=entry.getKey();
            if(page.isDirty()!=null&&page.isDirty().equals(tid)){
                flushPage(pid);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        Iterator it=findPage.entrySet().iterator();
        while(it.hasNext()){
            Map.Entry entry=(Map.Entry)it.next();
            Page page=(Page)entry.getValue();
            PageId pid=(PageId)entry.getKey();
            if(page.isDirty()==null){
                discardPage(pid);break;
            }
        }
    }

}

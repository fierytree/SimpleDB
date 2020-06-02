package simpledb;

import java.io.*;

import java.util.*;
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
    private ConcurrentHashMap<PageId,Integer> pages;
    int age;
    public class Lock{
        TransactionId tid;
        int type;
        Lock(){}
        Lock(TransactionId tid,int type){
            this.tid=tid;this.type=type;
        }
        public boolean equals(Object p){
            if(!(p instanceof Lock))return false;
            Lock q=(Lock)p;
            return q.tid.equals(tid);
        }
    }
    public class node{
        TransactionId tid;
        Vector<node> dep;
        ConcurrentHashMap<node,PageId>depPage;
        node(){}
        node(TransactionId t){
            tid=t;dep=new Vector<>();depPage=new ConcurrentHashMap<>();
        }

        public boolean equals(Object p){
            if(!(p instanceof node))return false;
            node q=(node)p;
            return q.tid.equals(tid);
        }
    }
    public class LockManager{
        final ConcurrentHashMap<PageId, Vector<Lock>> lockMap;
        final Vector<node> g=new Vector<>();
        LockManager(){
            lockMap=new ConcurrentHashMap<>(numPages);
        }
        public synchronized node findNode(TransactionId tid){
            node p=new node(tid);
                for(node n:g)
                    if(n.tid.equals(tid))return n;
            return p;
        }
        public synchronized boolean acquireLock(TransactionId tid,PageId pid,int type){
            if(lockMap.containsKey(pid)){
                Vector<Lock> locks = lockMap.get(pid);
                for (Lock lock : locks) {
                    if (lock.tid.equals(tid)) {
                        if(lock.type==type||lock.type==1)return true;
                        if(locks.size()==1) {
                            lock.type = 1;return true;
                        }
                        else {
                            node p=findNode(tid);
                            if(!g.contains(p))g.add(p);
                            for(Lock lock2:locks){
                                if(!lock2.tid.equals(tid)){
                                    node tmp=findNode(lock2.tid);
                                    if(!g.contains(tmp))g.add(tmp);
                                    if(!p.dep.contains(tmp)){
                                        p.dep.add(tmp);
                                        p.depPage.put(tmp,pid);
                                    }
                                    else p.depPage.replace(tmp,pid);
                                }
                            }
                            return false;
                        }
                    }
                    if (lock.type == 1 || type == 1) {
                        node p=findNode(tid);
                        node q=findNode(lock.tid);
                        if(!g.contains(p))g.add(p);
                        if(!g.contains(q))g.add(q);
                        if(!p.dep.contains(q)) {
                            p.dep.add(q);
                            p.depPage.put(q,pid);
                        }
                        else p.depPage.replace(q,pid);
                        return false;
                    }
                }
                locks.add(new Lock(tid, type));
                return true;
            }
            lockMap.put(pid, new Vector<>());
            lockMap.get(pid).add(new Lock(tid, type));
            return true;
        }
        public synchronized boolean det_cir(){
            Map<node,Integer> inDegree=new ConcurrentHashMap<>(g.size());
            for(node n:g){
                inDegree.put(n,0);
            }
            for(node n:g){
                for(node p:n.dep){
                    int num=inDegree.get(p);
                    inDegree.replace(p,num+1);
                }
            }
            Queue<node> queue=new LinkedList<>();
            for(node e:inDegree.keySet()){
                if(inDegree.get(e)==0){
                    queue.add(e);inDegree.remove(e);
                }
            }
            while(!queue.isEmpty()){
                node e=queue.poll();
                for(node n:e.dep){
                    int num=inDegree.get(n);
                    inDegree.replace(n,num-1);
                    if(num==1){queue.add(n);inDegree.remove(n);}
                }
            }
            return inDegree.size() != 0;
        }
        public synchronized void updateGraph(TransactionId tid){
            node n=findNode(tid);
            if(!g.contains(n))return;
            for(node p:g){
                p.dep.remove(n);
                p.depPage.remove(n);
            }
            g.remove(n);
        }
        public synchronized void updateGraph(TransactionId tid,PageId pid){
            node n=findNode(tid);
            if(!g.contains(n))return;
            Vector<Lock> locks=lockManager.lockMap.get(pid);
            for(Lock lock:locks){
                if(lock.tid.equals(tid)){
                    if(lock.type==0)return;
                }
            }
            for(node p:g){
                if(p.dep.contains(n)&&p.depPage.get(n).equals(pid)){
                    p.dep.remove(n);
                    p.depPage.remove(n);
                }
            }
        }
        public synchronized void removeLock(TransactionId tid,PageId pid){
            for(Lock lock:lockManager.lockMap.get(pid)){
                if(lock.tid.equals(tid)){
                    if(lockManager.lockMap.get(pid).size()==1)
                        lockManager.lockMap.remove(pid);
                    else lockManager.lockMap.get(pid).remove(lock);
                    break;
                }
            }
        }
    }
    private final LockManager lockManager;
    public LockManager getLockManager(){return lockManager;}
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
        this.numPages=numPages;
        lockManager=new LockManager();
        pages=new ConcurrentHashMap<>(numPages);
        age=0;
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
        while(!lockManager.acquireLock(tid,pid,perm.permLevel)){
            if(lockManager.det_cir())
                throw new TransactionAbortedException();
            try{Thread.sleep(20);}catch (Exception e){e.printStackTrace();}
        }
        if(findPage.containsKey(pid)) return findPage.get(pid);
        while (findPage.size() >= numPages)
            evictPage();
        DbFile dbfile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page p = dbfile.readPage(pid);
        findPage.put(pid, p);
        pages.put(pid,age++);
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
        lockManager.removeLock(tid,pid);
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
        synchronized (lockManager.lockMap) {
            for (Lock lock : lockManager.lockMap.get(p)) {
                if (lock.tid.equals(tid)) {
                    return true;
                }
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
                    pages.put(pid,age++);
                }
            }
        }

        for(PageId pid:lockManager.lockMap.keySet()){
            if(holdsLock(tid,pid)){
                releasePage(tid,pid);
            }
        }
        synchronized (lockManager.g){
            lockManager.updateGraph(tid);
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
            if(findPage.contains(page)) findPage.replace(page.getId(),page);
            else {
                findPage.put(page.getId(),page);
                this.pages.put(page.getId(),age++);
            }
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
            page.markDirty(true,tid);
            if(findPage.contains(page)) findPage.replace(page.getId(),page);
            else {
                findPage.put(page.getId(),page);
                this.pages.put(page.getId(),age++);
            }
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
        pages.remove(pid);
        synchronized (lockManager.lockMap) {
            lockManager.lockMap.remove(pid);
        }
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
        int n=200000001;PageId p=null;
        while(it.hasNext()){
            Map.Entry entry=(Map.Entry)it.next();
            Page page=(Page)entry.getValue();
            PageId pid=(PageId)entry.getKey();
            if(page.isDirty()==null){
                if(pages.get(pid)<n){
                    n=pages.get(pid);
                    p=pid;
                }
            }
        }
        if(p!=null)
            discardPage(p);
        else
            throw new DbException("none page to be evicted");
    }

}

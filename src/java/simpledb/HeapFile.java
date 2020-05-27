package simpledb;

import java.io.*;
import java.lang.reflect.Array;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File file;
    private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        file=f;tupleDesc=td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
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
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    // see DbFile.java for javadocs

    public Page readPage(PageId pid) {
        try{
            FileInputStream bis=new FileInputStream(file);
            bis.skip(pid.getPageNumber()*BufferPool.getPageSize());
            byte[] data =new byte[BufferPool.getPageSize()];
            bis.read(data,0,BufferPool.getPageSize());
            HeapPageId id = new HeapPageId(pid.getTableId(),pid.getPageNumber());
            bis.close();
            return new HeapPage(id,data);
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }


    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        try{
            RandomAccessFile raf=new RandomAccessFile(file,"rw");
            raf.seek(page.getId().getPageNumber()*BufferPool.getPageSize());
            raf.write(page.getPageData());
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int)file.length()/BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> pages=new ArrayList<>();
        for(int i=0;i<numPages();i++){
            PageId pid2=new HeapPageId(this.getId(),i);
            HeapPage p=(HeapPage)Database.getBufferPool().getPage(tid, pid2, Permissions.READ_WRITE);
            if(p.getNumEmptySlots()==0){
                continue;
            }
            Database.getBufferPool().releasePage(tid,p.getId());
            p.insertTuple(t);
            pages.add(p);
            return pages;
        }
        byte[] data=HeapPage.createEmptyPageData();
        HeapPageId pid=new HeapPageId(this.getId(),numPages());
        HeapPage page=new HeapPage(pid,data);
        page.insertTuple(t);
        writePage(page);
        pages.add(page);
        return pages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        PageId pid=new HeapPageId(this.getId(),t.getRecordId().getPageId().getPageNumber());
        HeapPage p=(HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        p.deleteTuple(t);
        ArrayList<Page> pages=new ArrayList<>();
        pages.add(p);
        return pages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HFileIterator(this,tid);
    }
    private static final class HFileIterator implements DbFileIterator{
        private int pageNo;
        private final TransactionId tid;
        private Iterator<Tuple> it;
        private final HeapFile heapFile;

        HFileIterator(HeapFile file, TransactionId tid){
            this.heapFile = file;
            this.tid = tid;
        }
        @Override
        public void open() throws DbException, TransactionAbortedException {
            pageNo=0;
            HeapPageId pid = new HeapPageId(heapFile.getId(),pageNo);
            HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            it = page.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(it == null){
                return false;
            }

            if(!it.hasNext()){
                if(pageNo < heapFile.numPages()-1){
                    pageNo++;
                    HeapPageId pid = new HeapPageId(heapFile.getId(),pageNo);
                    HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
                    it = page.iterator();
                    return it.hasNext();
                }else return false;
            }
            return true;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(it == null || !it.hasNext()){
                throw new NoSuchElementException();
            }
            return it.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();open();
        }

        @Override
        public void close() {
            it = null;
        }

    }
}


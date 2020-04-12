package simpledb;

import java.util.ArrayList;
import java.util.List;

import static simpledb.Type.INT_TYPE;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private OpIterator child;
    private int tableId;
    private TupleDesc td;
    boolean complete;

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
        tid=t;this.child=child;this.tableId=tableId;
        TupleDesc tpd=Database.getCatalog().getTupleDesc(tableId);
        if(!tpd.equals(child.getTupleDesc()))
            throw new DbException("unmatched tuple");
        this.td=new TupleDesc(new Type[]{Type.INT_TYPE});
        complete=false;
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();super.open();
    }

    public void close() {
        child.close();super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
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
        if(complete)
            return null;
        complete=true;
        int num=0;
        try{
            while(child.hasNext()){
                Database.getBufferPool().insertTuple(tid,tableId,child.next());
                num++;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        Tuple t=new Tuple(new TupleDesc(new Type[]{INT_TYPE}));
        t.setField(0,new IntField(num));
        return t;
    }

    @Override
    public OpIterator[] getChildren() {
        OpIterator[] opIterators;
        List<OpIterator> tmp=new ArrayList<>();
        try{
            while(child.hasNext()){
                tmp.add(child);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        opIterators=new OpIterator[tmp.size()];
        for(int i=0;i<tmp.size();i++){
            opIterators[i]=tmp.get(i);
        }
        return opIterators;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        int i=0;
        try{
            while(child.hasNext()){
                child=children[i++];
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}

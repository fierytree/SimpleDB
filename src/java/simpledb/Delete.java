package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static simpledb.Type.INT_TYPE;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private OpIterator child;
    private TupleDesc td;
    boolean complete;

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
        tid=t;this.child=child;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(complete)
            return null;
        complete=true;
        int n=0;
        try{
            while(child.hasNext()){
                Database.getBufferPool().deleteTuple(tid,child.next());
                n++;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        Tuple t=new Tuple(new TupleDesc(new Type[]{INT_TYPE}));
        t.setField(0,new IntField(n));
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

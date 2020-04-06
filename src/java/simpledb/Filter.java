package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    private Predicate predicate;
    private OpIterator child;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        predicate=p;this.child=child;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public TupleDesc getTupleDesc() {
        return child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        child.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        while(child.hasNext()){
            Tuple t=child.next();
            if(predicate.filter(t))
                return t;
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        List<OpIterator> a=new ArrayList<>();
        try{
            do{
                a.add(child);
            }
            while(child.hasNext());
        }catch (Exception e){
            e.printStackTrace();
        }
        OpIterator[] b=new OpIterator[a.size()];
        for(int i=0;i<a.size();i++){
            b[i]=a.get(i);
        }
        return b;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        int i=0;
        try{
            do{
                child=children[i++];
            }
            while(child.hasNext());
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}

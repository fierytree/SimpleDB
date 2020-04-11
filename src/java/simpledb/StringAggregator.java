package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static simpledb.Type.INT_TYPE;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Map<Field,Integer> result;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.afield=afield;this.gbfield=gbfield;this.what=what;this.gbfieldtype=gbfieldtype;
        result=new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
//        if(what!=Op.COUNT)
//            throw new IllegalArgumentException();
        Field tmp=tup.getField(gbfield);
        StringField v=(StringField)tup.getField(afield);
        String value=v.getValue();
        if (!result.containsKey(tmp)){
            result.put(tmp, 1);
        }
        else{
            result.replace(tmp,result.get(tmp)+1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        return new StringAggIterator();
    }
    private class StringAggIterator implements OpIterator{
        private Iterator<Map.Entry<Field, Integer>> it;
        private TupleDesc td;
        StringAggIterator(){
            it=result.entrySet().iterator();
            if(gbfieldtype==null){
                td=new TupleDesc(new Type[]{INT_TYPE},new String[]{"aggregateVal"});
            }
            else{
                td=new TupleDesc(new Type[]{gbfieldtype,INT_TYPE},new String[]{"groupVal","aggregateVal"});
            }
        }

        @Override
        public TupleDesc getTupleDesc() {
            return td;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {

        }

        @Override
        public void close() {
            it=null;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            it=result.entrySet().iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return it.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException{
            Tuple t=new Tuple(td);
            if(gbfieldtype==null){
                t.setField(0,new IntField(it.next().getValue()));
            }
            else{
                Map.Entry<Field, Integer> entry=it.next();
                t.setField(0,entry.getKey());
                t.setField(1,new IntField(entry.getValue()));
            }
            return t;
        }
    }

}

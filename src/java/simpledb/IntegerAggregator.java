package simpledb;

import java.util.*;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static simpledb.Type.INT_TYPE;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Map<Field,Integer> result;
    private Map<Field,Double> AvgResult;
    private Map<Field,Integer> CountResult;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.afield=afield;this.gbfield=gbfield;this.what=what;this.gbfieldtype=gbfieldtype;
        result=new HashMap<>();
        AvgResult=new HashMap<>();
        CountResult=new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field tmp = gbfield == NO_GROUPING ? null : tup.getField(gbfield);
        IntField v=(IntField)tup.getField(afield);
        int value=v.getValue();
        switch(what) {
            case AVG:
                if (!result.containsKey(tmp)) {
                    result.put(tmp, value);
                    CountResult.put(tmp,1);
                    AvgResult.put(tmp, (double)value);
                }
                else {
                    result.replace(tmp, result.get(tmp)+value);
                    CountResult.replace(tmp,CountResult.get(tmp)+1);
                    double x=((double)result.get(tmp))/CountResult.get(tmp);
                    AvgResult.replace(tmp,x);
                }break;
            case MAX:
                if (!result.containsKey(tmp)){
                    result.put(tmp, value);
                }
                else{
                    result.replace(tmp,max(result.get(tmp),value));
                }break;
            case MIN:
                if (!result.containsKey(tmp)){
                    result.put(tmp, value);
                }
                else{
                    result.replace(tmp,min(result.get(tmp),value));
                }break;
            case COUNT:
                if (!result.containsKey(tmp)){
                    result.put(tmp, 1);
                }
                else{
                    result.replace(tmp,result.get(tmp)+1);
                }break;
            case SUM:
                if (!result.containsKey(tmp)){
                    result.put(tmp, value);
                }
                else{
                    result.replace(tmp,result.get(tmp)+value);
                }break;
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        return new IntAggIterator();
    }
    private class IntAggIterator implements OpIterator{
        private Iterator<Map.Entry<Field, Integer>> it;
        private Iterator<Map.Entry<Field, Double>> it2;
        private Op type;
        private TupleDesc td;
        IntAggIterator(){
            type=what;
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
            if(type==Op.AVG){
                it2=AvgResult.entrySet().iterator();
            }
            it=result.entrySet().iterator();
        }

        @Override
        public void close() {
            it=null;
            it2=null;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if(type==Op.AVG){
                it2=AvgResult.entrySet().iterator();
            }
            it=result.entrySet().iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(type==Op.AVG){
                return it2.hasNext();
            }
            return it.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            Tuple t=new Tuple(td);
            if(type==Op.AVG){
                Map.Entry<Field, Double> entry=it2.next();
                if(gbfieldtype==null){
                    t.setField(0,new IntField(entry.getValue().intValue()));
                }
                else{
                    t.setField(0,entry.getKey());
                    t.setField(1,new IntField(entry.getValue().intValue()));
                }

            }
            else{
                Map.Entry<Field, Integer> entry=it.next();
                if(gbfieldtype==null){
                    t.setField(0,new IntField(entry.getValue()));
                }
                else{
                    t.setField(0,entry.getKey());
                    t.setField(1,new IntField(entry.getValue()));
                }
            }
            return t;
        }
    }

}

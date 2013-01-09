package storm.trident.state;

import java.util.List;
import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;

public class ReducerValueUpdater implements ValueUpdater<Object> {
    List<TridentTuple> tuples;
    ReducerAggregator agg;
    
    public ReducerValueUpdater(ReducerAggregator agg, List<TridentTuple> tuples) {
        this.agg = agg;
        this.tuples = tuples;
    }

    @Override
    public Object update(Object stored) {
      /* Is this correct?
       * If stored becomes null on an earlier reduction are we re-initialising? */
        Object ret = (stored == null) ? this.agg.init() : stored;
        for(TridentTuple t: tuples) {
           ret =  this.agg.reduce(ret, t);
        }
        return ret;
    }        
}
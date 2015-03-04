package ar.com.carloscurotto.storm.updates.trident;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import ar.com.carloscurotto.storm.updates.trident.repository.HBaseUpdateCountsRepository;

public class HBaseFunction extends BaseFunction {

    private static final long serialVersionUID = 1L;

    private HBaseUpdateCountsRepository counts;

    public HBaseFunction(HBaseUpdateCountsRepository theCounts) {
        this.counts = theCounts;
    }

    @Override
    public void execute(TridentTuple theTuple, TridentCollector theCollector) {
        String update = theTuple.getString(0);
        Integer count = counts.get(update);
        if (count == null) {
            count = 1;
        } else {
            count++;
        }
        counts.put(update, count);
        System.out.println("Update tuple received [" + update + ", " + count + "]");
    }

}

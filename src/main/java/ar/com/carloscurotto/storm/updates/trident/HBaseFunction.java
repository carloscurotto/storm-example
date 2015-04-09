package ar.com.carloscurotto.storm.updates.trident;

import java.util.Map;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import ar.com.carloscurotto.storm.updates.trident.repository.HBaseUpdateCountsRepository;

public class HBaseFunction extends BaseFunction {

    private static final long serialVersionUID = 1L;

    private HBaseUpdateCountsRepository counts;

    public HBaseFunction(HBaseUpdateCountsRepository theCounts) {
        this.counts = theCounts;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        this.counts.start();
    }

    @Override
    public void cleanup() {
        this.counts.stop();
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
        System.out.println("HBase update [" + update + ", " + count + "] by thread ["
                + Thread.currentThread().getName() + "]");
    }

}

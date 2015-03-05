package ar.com.carloscurotto.storm.updates.trident;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

public class ExternalInternalFilter extends BaseFilter {

    private static final long serialVersionUID = 1L;

    @Override
    public boolean isKeep(TridentTuple theTuple) {
        String update = theTuple.getString(0);
        if (update.contains("gloss")) {
            System.out
                    .println("External internal tuple detected by thread [" + Thread.currentThread().getName() + "]");
            return true;
        }
        return false;
    }
}

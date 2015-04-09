package ar.com.carloscurotto.storm.updates.kafka.repository;

public class HBaseUpdateCountsRepository extends AbstractUpdateCountsRepository {

    private static final long serialVersionUID = 1L;

    private static final String HZ_MAP_NAME = "hbase-update-count-map";

    @Override
    protected String getMapName() {
        return HZ_MAP_NAME;
    }

}

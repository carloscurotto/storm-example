package ar.com.carloscurotto.storm.updates.repository;

public class GlossUpdateCountsRepository extends AbstractUpdateCountsRepository {

    private static final long serialVersionUID = 1L;

    private static final String HZ_MAP_NAME = "gloss-update-count-map";

    @Override
    protected String getMapName() {
        return HZ_MAP_NAME;
    }

}

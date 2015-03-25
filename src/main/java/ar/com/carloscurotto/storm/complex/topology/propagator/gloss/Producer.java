package ar.com.carloscurotto.storm.complex.topology.propagator.gloss;

import ar.com.carloscurotto.storm.complex.service.Closeable;
import ar.com.carloscurotto.storm.complex.service.Openable;

public interface Producer<T> extends Closeable, Openable {
    public void send(T message);
}

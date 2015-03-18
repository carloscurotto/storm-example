package ar.com.carloscurotto.storm.complex.service;

/**
 * Represents a component that can be closed. The close method is invoked to release associated resources.
 *
 * @author O605461
 */
public interface Closeable {

	/**
	 * Performs the required actions needed to close the component from being used. Once this method is called, the
	 * component will not be able work anymore. It can be called many times, after the first time it will have no
	 * effect.
	 */
	public void close();
}

package ar.com.carloscurotto.storm.complex.service;

/**
 * Represents a component that can be opened. The open method is invoked to acquire associated resources.
 *
 * @author O605461
 */
public interface Openable {

	/**
	 * Performs the required actions needed for to start working. It should be called only once, if it is called more
	 * than once it will raise an exception.
	 */
	public void open();

	/**
	 * Tells if the component has been opened. It is a query method, it can be called many times without changing the
	 * state of the component.
	 *
	 * @return true if open has already been called, otherwise false.
	 */
	public boolean isOpen();

}

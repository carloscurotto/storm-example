package ar.com.carloscurotto.storm.complex.topology.route;

/**
 * Enumerates all the possible update routes that our topology can handle.
 *
 * @author O605461
 *
 */
public enum UpdateRoute {

    /**
     * Tells our topology that the updates should be propagated externally first and then internally.
     */
    EXTERNAL_INTERNAL,

    /**
     * Tells our topology that the updates should be propagated internally only.
     */
    INTERNAL_ONLY;

}
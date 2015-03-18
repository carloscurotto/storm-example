package ar.com.carloscurotto.storm.complex.topology.propagator.gloss;

//import com.jpmorgan.cib.toolkit.util.CtkApplicationUtil;

/**
 * Holds the different configuration parameters for queue creation.
 *
 * @author D540601
 */
public class JMSConfiguration {
    private static final String DESTINATION_PREFIX = "/Destination[@name='GLOSS']";

    /**
     * Returns the host name.
     *
     * @return the host name.
     */
    public static String getHostName() {
        //return CtkApplicationUtil.getProperty(DESTINATION_PREFIX + "/hostname");
        return null;
    }

    /**
     * Returns the port.
     *
     * @return returns the port.
     */
    public static String getPort() {
        //return CtkApplicationUtil.getProperty(DESTINATION_PREFIX + "/port");
        return null;
    }

    /**
     * Returns the QManager.
     *
     * @return the QManager.
     */
    public static String getQManager() {
        //return CtkApplicationUtil.getProperty(DESTINATION_PREFIX + "/qmgr");
        return null;
    }

    /**
     * Returns the QName.
     *
     * @return the QName.
     */
    public static String getQName() {
        //return CtkApplicationUtil.getProperty(DESTINATION_PREFIX + "/queue");
        return null;
    }

    /**
     * Returns the channel.
     *
     * @return the channel.
     */
    public static String getChannel() {
        //return CtkApplicationUtil.getProperty(DESTINATION_PREFIX + "/channel");
        return null;
    }

    /**
     * Returns the transport type.
     *
     * @return the transport type.
     */
    public static String getTransportType() {
        //return CtkApplicationUtil.getProperty(DESTINATION_PREFIX + "/transportType");
        return null;
    }

    /**
     * Returns the ssl cipher suite.
     *
     * @return the ssl cipher suite.
     */
    public static String getSslCipherSuite() {
        //return CtkApplicationUtil.getProperty(DESTINATION_PREFIX + "/sslciphersuite");
        return null;
    }
}
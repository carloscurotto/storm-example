package ar.com.carloscurotto.storm.complex.topology.propagator.gloss;

import ar.com.carloscurotto.storm.complex.service.OpenAwareProducer;

/**
 * This is a producer that will write any string that is given in the console.
 * Intended for testing and demo processes.
 * @author d540601
 */
public class ConsolePrintMessageProducer extends OpenAwareProducer<String>{

    /**
     * Writes theContext to the console.
     * @param theContext a string to write to the output console.
     */
    @Override
    protected void doSend(String theContext) {
        System.out.println(theContext);
    }

    @Override
    protected void doOpen() {
        // no implementation
    }

    @Override
    protected void doClose() {
        // no implementation
    }

}

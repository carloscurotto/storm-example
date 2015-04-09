package ar.com.carloscurotto.storm.complex.topology.propagator.executor;

import static org.junit.Assert.assertEquals;

import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.test.util.ReflectionTestUtils;

import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import ar.com.carloscurotto.storm.complex.topology.propagator.provider.UpdatePropagatorProvider;

@RunWith(EasyMockRunner.class)
public class AbstractUpdatePropagatorExecutorTest {
    private static class AbstractUpdatePropagatorExecutorConcrete extends AbstractUpdatePropagatorExecutor {
        private static final long serialVersionUID = 1L;

        public AbstractUpdatePropagatorExecutorConcrete(
                UpdatePropagatorProvider thePropagatorProvider) {
            super(thePropagatorProvider);
        }

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {}
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();
    
    @Mock
    private UpdatePropagatorProvider updatePropagatorProviderMock;
        
    @Test
    public void constructorWithValidParameters() {
        AbstractUpdatePropagatorExecutor executor = new AbstractUpdatePropagatorExecutorConcrete(updatePropagatorProviderMock);
        UpdatePropagatorProvider provider = (UpdatePropagatorProvider)ReflectionTestUtils.getField(executor, "propagatorProvider");
        assertEquals(updatePropagatorProviderMock, provider);
    }
    
    @Test
    public void constructorWithNullPropagatorProvider() {
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("The propagator provider cannot be null");
        
        new AbstractUpdatePropagatorExecutorConcrete(null);
    }
}
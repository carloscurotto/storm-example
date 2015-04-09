package ar.com.carloscurotto.storm.complex.topology.propagator.executor;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.test.util.ReflectionTestUtils;

import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import ar.com.carloscurotto.storm.complex.model.Update;
import ar.com.carloscurotto.storm.complex.model.UpdateRow;
import ar.com.carloscurotto.storm.complex.service.OpenAwarePropagator;
import ar.com.carloscurotto.storm.complex.topology.propagator.context.UpdatePropagatorContext;
import ar.com.carloscurotto.storm.complex.topology.propagator.provider.UpdatePropagatorProvider;
import ar.com.carloscurotto.storm.complex.topology.propagator.result.UpdatePropagatorResult;
import backtype.storm.tuple.Values;

@RunWith(EasyMockRunner.class)
public class ExternalUpdatePropagatorExecutorTest extends EasyMockSupport {
    
    @Mock
    private UpdatePropagatorProvider updatePropagatorProviderMock;
    
    @Mock
    private TridentTuple tridentTupleMock;
    
    @Mock
    private TridentCollector tridentCollectorMock;
    
    @Mock
    private Update updateMock;
    
    @Mock
    private UpdateRow updateRowMock;
    
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    
    private OpenAwarePropagator<UpdatePropagatorContext, UpdatePropagatorResult> propagator = 
            new OpenAwarePropagator<UpdatePropagatorContext, UpdatePropagatorResult>(){
              private static final long serialVersionUID = 1L;

              @Override
              protected UpdatePropagatorResult doPropagate(UpdatePropagatorContext theContext) {
                  return UpdatePropagatorResult.createSuccess("theMessage");
              }

              @Override
              protected void doOpen() {}

              @Override
              protected void doClose() {}
            };

    @Test
    public void constructorWithValidArguments() {
        ExternalUpdatePropagatorExecutor executor = new ExternalUpdatePropagatorExecutor(updatePropagatorProviderMock, new HashSet<String>());
        assertEquals(updatePropagatorProviderMock, ReflectionTestUtils.getField(executor, "propagatorProvider"));
        assertEquals(new HashSet<String>(), ReflectionTestUtils.getField(executor, "skipableSystemIds"));
    }
    
    @Test 
    public void constructorWithNullSkipableSystemIds() {
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("The set of system ids to be skipped can not be null.");
        
        new ExternalUpdatePropagatorExecutor(updatePropagatorProviderMock, null);
    }
            
    @Test
    public void executeWithValidArgumentsExecutesPropagatorWithZeroRows() {
        ExternalUpdatePropagatorExecutor executor = new ExternalUpdatePropagatorExecutor(updatePropagatorProviderMock, new HashSet<String>());
               
        expect(tridentTupleMock.getValueByField("update")).andReturn(updateMock);
        expect(updateMock.getSystemId()).andReturn("theId").times(2);
        
        Collection<UpdateRow> updateRows = new ArrayList<UpdateRow>();
        updateRows.add(updateRowMock);
        expect(updateMock.getRows()).andReturn(updateRows).times(2);
        
        expect(updatePropagatorProviderMock.getPropagator("theId")).andReturn(propagator);
        propagator.open();
        expect(updateMock.getTableName()).andReturn("theTableName");
        expect(updateMock.getParameters()).andReturn(new HashMap<String, Object>());
        expect(updateRowMock.getId()).andReturn("update-row-id");

        expect(updateMock.getId()).andReturn("update-id");
        
        tridentCollectorMock.emit(anyObject(Values.class));
        expectLastCall();
        
        replayAll();
        executor.execute(tridentTupleMock, tridentCollectorMock);
        verifyAll();
    }
    
    @Test
    public void executeWithValidArgumentsAndCreateSkipResultRows() {
        Set<String> theSkipableSystemIds = new HashSet<String>();
        theSkipableSystemIds.add("the update id");
        
        ExternalUpdatePropagatorExecutor executor = new ExternalUpdatePropagatorExecutor(updatePropagatorProviderMock, theSkipableSystemIds);
               
        expect(tridentTupleMock.getValueByField("update")).andReturn(updateMock);
        expect(updateMock.getSystemId()).andReturn("the update id").times(1);
        
        Collection<UpdateRow> updateRows = new ArrayList<UpdateRow>();
        updateRows.add(updateRowMock);
        expect(updateRowMock.getId()).andReturn("update-row-id");
        expect(updateMock.getRows()).andReturn(updateRows).times(2);
        
        expect(updateMock.getId()).andReturn("update-id");
        
        tridentCollectorMock.emit(anyObject(Values.class));
        expectLastCall();
        
        replayAll();
        executor.execute(tridentTupleMock, tridentCollectorMock);
        verifyAll();
    }
}

package ar.com.carloscurotto.storm.complex.topology.propagator.executor;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.Test;
import org.junit.runner.RunWith;

import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import ar.com.carloscurotto.storm.complex.model.Result;
import ar.com.carloscurotto.storm.complex.model.ResultRow;
import ar.com.carloscurotto.storm.complex.model.Update;
import ar.com.carloscurotto.storm.complex.model.UpdateRow;
import ar.com.carloscurotto.storm.complex.service.OpenAwarePropagator;
import ar.com.carloscurotto.storm.complex.topology.propagator.context.UpdatePropagatorContext;
import ar.com.carloscurotto.storm.complex.topology.propagator.provider.UpdatePropagatorProvider;
import ar.com.carloscurotto.storm.complex.topology.propagator.result.UpdatePropagatorResult;
import backtype.storm.tuple.Values;


@RunWith(EasyMockRunner.class)
public class InternalUpdatePropagatorExecutorTest extends EasyMockSupport {

    @Mock
    private TridentTuple theTuple;
    
    @Mock
    private TridentCollector theCollector;
    
    @Mock
    private Update update;
    
    @Mock
    private Result externalResult;
    
    @Mock
    private UpdatePropagatorProvider thePropagatorProvider;
    
    //@Mock
    //private UpdatePropagatorProvider propagatorProviderMock;
    
    private OpenAwarePropagator<UpdatePropagatorContext, UpdatePropagatorResult> propagator =
            new OpenAwarePropagator<UpdatePropagatorContext, UpdatePropagatorResult>() {
                private static final long serialVersionUID = 1L;

                @Override
                protected UpdatePropagatorResult doPropagate(UpdatePropagatorContext theContext) {
                    return UpdatePropagatorResult.createSuccess("theMessage");
                }

                @Override
                protected void doOpen() {
                    // TODO Auto-generated method stub
                    
                }

                @Override
                protected void doClose() {
                    // TODO Auto-generated method stub
                    
                }
        
    };
    
    @Test
    public void executeWithSuccessfulExternalResultRow() {
        expect(theTuple.getValueByField("update")).andReturn(update);
        expect(theTuple.getValueByField("external-result")).andReturn(externalResult);
        
        Collection<UpdateRow> updateRows = new ArrayList<UpdateRow>();
        UpdateRow updateRow = new UpdateRow("the updateRow id", 1l, new HashMap<String, Object>(), new HashMap<String, Object>());
        updateRows.add(updateRow);
        expect(update.getRows()).andReturn(updateRows);
        
        ResultRow resultRow = ResultRow.success("resultRow id", "theMessage");
        expect(externalResult.getRow("the updateRow id")).andReturn(resultRow);
        
        // enters process(update, updateRow)
        expect(update.getSystemId()).andReturn("theSystemId");
        propagator.open();
        expect(thePropagatorProvider.getPropagator("theSystemId")).andReturn(propagator);
        expect(update.getTableName()).andReturn("table name");
        expect(update.getParameters()).andReturn(new HashMap<String, Object>());
        // out from process
        
        expect(update.getId()).andReturn("update-id");
        theCollector.emit(anyObject(Values.class));
        expectLastCall();
        
        InternalUpdatePropagatorExecutor executor = new InternalUpdatePropagatorExecutor(thePropagatorProvider);
        
        replayAll();
        executor.execute(theTuple, theCollector);
        verifyAll();
    }
    
}

package ar.com.carloscurotto.storm.complex.topology;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import ar.com.carloscurotto.storm.complex.topology.propagator.executor.ExternalUpdatePropagatorExecutor;
import ar.com.carloscurotto.storm.complex.topology.propagator.executor.InternalUpdatePropagatorExecutor;
import ar.com.carloscurotto.storm.complex.topology.propagator.executor.ResultUpdatePropagatorExecutor;
import ar.com.carloscurotto.storm.complex.topology.spout.UpdatesSpoutFactory;

import com.google.common.base.Preconditions;

public class UpdatesTopologyConfiguration {

    private static final String ADP_UDPATE_CONTEXT_PATH = "complex-context.xml";
    private static final String UPDATES_SPOUT_FACTORY = "UpdatesSpoutFactory";
    private static final String EXTERNAL_UPDATE_PROPAGATOR_EXECUTOR = "ExternalUpdatePropagatorExecutor";
    private static final String INTERNAL_UPDATE_PROPAGATOR_EXECUTOR = "InternalUpdatePropagatorExecutor";
    private static final String RESULT_UPDATE_PROPAGATOR_EXECUTOR = "ResultUpdatePropagatorExecutor";

    private ApplicationContext context;
    
    public UpdatesTopologyConfiguration() {
        context = new ClassPathXmlApplicationContext(ADP_UDPATE_CONTEXT_PATH);
        Preconditions.checkState(context != null, "The updates topology configuration context can not be null.");
    }

    public UpdatesSpoutFactory getUpdatesSpoutFactory() {
        //return new FixedUpdatesSpoutFactory();
        return (UpdatesSpoutFactory) context.getBean(UPDATES_SPOUT_FACTORY);
    }

    public ExternalUpdatePropagatorExecutor getExternalUpdatePropagatorExecutor() {
//        Map<String, OpenAwareService<UpdatePropagatorContext, ResultRow>> propagators =
//                new HashMap<String, OpenAwareService<UpdatePropagatorContext, ResultRow>>();
//        propagators.put("SEMS", new PrintExternalUpdatePropagator());
//        UpdatePropagatorProvider propagatorProvider = new UpdatePropagatorProvider(propagators);
//        
//        Map<String, UpdateRoute> routes = new HashMap<String, UpdateRoute>();
//        routes.put("SEMS", UpdateRoute.EXTERNAL_INTERNAL);
//        routes.put("ANOTHER", UpdateRoute.INTERNAL_ONLY);
//        UpdateRouteProvider routeProvider = new UpdateRouteProvider(routes);
//        
//        return new ExternalUpdatePropagatorExecutor(propagatorProvider, routeProvider);
        return (ExternalUpdatePropagatorExecutor) context.getBean(EXTERNAL_UPDATE_PROPAGATOR_EXECUTOR);
    }

    public InternalUpdatePropagatorExecutor getInternalUpdatePropagatorExecutor() {
//        Map<String, OpenAwareService<UpdatePropagatorContext, ResultRow>> propagators =
//                new HashMap<String, OpenAwareService<UpdatePropagatorContext, ResultRow>>();
//        propagators.put("SEMS", new PrintInternalUpdatePropagator());
//        propagators.put("ANOTHER", new PrintInternalUpdatePropagator());
//        
//        UpdatePropagatorProvider propagatorProvider = new UpdatePropagatorProvider(propagators);
//        
//        return new InternalUpdatePropagatorExecutor(propagatorProvider);
        return (InternalUpdatePropagatorExecutor) context.getBean(INTERNAL_UPDATE_PROPAGATOR_EXECUTOR);
    }

    public ResultUpdatePropagatorExecutor getResultUpdatePropagatorExecutor() {
//        return new ResultUpdatePropagatorExecutor();
        return (ResultUpdatePropagatorExecutor) context.getBean(RESULT_UPDATE_PROPAGATOR_EXECUTOR);
    }
}

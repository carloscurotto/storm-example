package ar.com.carloscurotto.storm.complex.topology.route.provider;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import ar.com.carloscurotto.storm.complex.topology.route.UpdateRoute;

import com.google.common.base.Preconditions;

/**
 * Provides the route that each update should follow inside our topology for each update id. We assume that each update
 * will have a particular id that will be used by this provider to know which route it should follow. For more
 * information about the supported routes see {@link UpdateRoute}.
 *
 * @author O605461
 *
 */
public class UpdateRouteProvider implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * All the possible update routes for each update id.
     */
    private Map<String, UpdateRoute> routes;

    /**
     * Creates an update router for the given routes.
     *
     * @param theRoutes
     *            the given update routes. It can not be null.
     */
    public UpdateRouteProvider(final Map<String, UpdateRoute> theRoutes) {
        Validate.notNull(theRoutes, "The routes can not be null.");
        routes = new HashMap<String, UpdateRoute>(theRoutes);
    }

    /**
     * Provides the {@link UpdateRoute} for the given id.
     *
     * @param theSystemId
     *            the given update's system id. It can not be blank.
     * @return the update route for the given id or {@link IllegalStateException} if there is no route registered for
     *         the given id.
     */
    public UpdateRoute getRoute(final String theSystemId) {
        Preconditions
                .checkArgument(StringUtils.isNotBlank(theSystemId), "The id can not be blank.");
        UpdateRoute route = routes.get(theSystemId);
        Preconditions.checkState(route != null, "Can not find a route for the update's system id ["
                + theSystemId + "]. Please, configure the routes properly.");
        return route;
    }

}
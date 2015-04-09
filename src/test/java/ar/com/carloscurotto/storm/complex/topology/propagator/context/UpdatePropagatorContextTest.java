package ar.com.carloscurotto.storm.complex.topology.propagator.context;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import ar.com.carloscurotto.storm.complex.model.UpdateRow;

public class UpdatePropagatorContextTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    
    @Test
    public void constructorWithValidArguments() {
        UpdateRow theRow = new UpdateRow("theId", 1l, new HashMap<String, Object>(), new HashMap<String, Object>());
        UpdatePropagatorContext ctx = new UpdatePropagatorContext("theTableName", theRow, new HashMap<String, Object>());
        
        assertEquals("theTableName", ctx.getTableName());
        assertEquals(theRow, ctx.getRow());
        assertEquals(new HashMap<String, Object>(), ctx.getParameters());
    }
    
    @Test
    public void constructorWithNullTableName() {
        UpdateRow theRow = new UpdateRow("theId", 1l, new HashMap<String, Object>(), new HashMap<String, Object>());
        
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("The table name can not be blank");
        
        new UpdatePropagatorContext(null, theRow, new HashMap<String, Object>());
        
    }
    
    @Test
    public void constructorWithEmptyTableName() {
        UpdateRow theRow = new UpdateRow("theId", 1l, new HashMap<String, Object>(), new HashMap<String, Object>());
        
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("The table name can not be blank");
        
        new UpdatePropagatorContext(StringUtils.EMPTY, theRow, new HashMap<String, Object>());
    }
    
    @Test
    public void constructorWithBlankTableName() {
        UpdateRow theRow = new UpdateRow("theId", 1l, new HashMap<String, Object>(), new HashMap<String, Object>());
        
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("The table name can not be blank");
        
        new UpdatePropagatorContext(" ", theRow, new HashMap<String, Object>());
    }
    
    @Test
    public void constructorWithNullUpdateRow(){
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("The row can not be null");
        
        new UpdatePropagatorContext("theTableName", null, new HashMap<String, Object>());
    }
    
    @Test
    public void constructorWithNullParameters(){
        UpdateRow theRow = new UpdateRow("theId", 1l, new HashMap<String, Object>(), new HashMap<String, Object>());
        
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("The parameters can not be null");
        
        new UpdatePropagatorContext("theTableName", theRow, null);
    }
    
    @Test
    public void hasParametersShouldReturnFalse(){
        UpdateRow theRow = new UpdateRow("theId", 1l, new HashMap<String, Object>(), new HashMap<String, Object>());
        UpdatePropagatorContext ctx = new UpdatePropagatorContext("theTableName", theRow, new HashMap<String, Object>());
        
        assertFalse(ctx.hasParameters());
    }
    
    @Test
    public void hasParametersShouldReturnTrue(){
        UpdateRow theRow = new UpdateRow("theId", 1l, new HashMap<String, Object>(), new HashMap<String, Object>());
        Map<String, Object> parameters = new HashMap<String, Object>();
        parameters.put("a key", "a value");
        UpdatePropagatorContext ctx = new UpdatePropagatorContext("theTableName", theRow, parameters);
        
        assertTrue(ctx.hasParameters());
    }
    
    @Test
    public void getParametersShouldReturnCorrectParameters() {
        UpdateRow theRow = new UpdateRow("theId", 1l, new HashMap<String, Object>(), new HashMap<String, Object>());
        Map<String, Object> theParameters = new HashMap<String, Object>();
        UpdatePropagatorContext ctx = new UpdatePropagatorContext("theTableName", theRow, theParameters);
        
        assertEquals(theParameters, ctx.getParameters());
    }
}

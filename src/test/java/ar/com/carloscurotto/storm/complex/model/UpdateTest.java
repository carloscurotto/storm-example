package ar.com.carloscurotto.storm.complex.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class UpdateTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();
    
    @Test
    public void constructorWithValidParameters(){
        Map<String, Object> theParameters = new HashMap<String, Object>();
        theParameters.put("a key", "a value");
        
        UpdateRow updateRow = new UpdateRow("update-row-id", new HashMap<String, Object>(), new HashMap<String, Object>());
        
        Collection<UpdateRow> updateRows = new ArrayList<UpdateRow>();
        updateRows.add(updateRow);
        
        Update update = new Update("update-id", "theSystemId", "theTableName", theParameters, updateRows);
        assertEquals("update-id", update.getId());
        assertEquals("theSystemId", update.getSystemId());
        assertEquals("theTableName", update.getTableName());
        assertEquals(theParameters, update.getParameters());
        assertEquals(updateRows.size(), update.getRows().size());
        for(UpdateRow uRow : update.getRows()) {
            assertTrue(updateRows.contains(uRow));
        }
    }
    
    @Test
    public void constructorWithNullId() {
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("The id can not be blank");
        
        new Update(null, "theSystemId", "theTableName", new HashMap<String, Object>(), new ArrayList<UpdateRow>());
    }
    
    @Test
    public void constructorWithEmtpyId() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("The id can not be blank");
        
        new Update("", "theSystemId", "theTableName", new HashMap<String, Object>(), new ArrayList<UpdateRow>());
    }
    
    @Test
    public void constructorWithBlankId() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("The id can not be blank");
        
        new Update(" ", "theSystemId", "theTableName", new HashMap<String, Object>(), new ArrayList<UpdateRow>());
    }
    
    @Test
    public void constructorWithNullSystemId() {
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("The system id can not be blank");
        
        new Update("theId", null, "theTableName", new HashMap<String, Object>(), new ArrayList<UpdateRow>());
    }
    
    @Test
    public void constructorWithEmptySystemId() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("The system id can not be blank");
        
        new Update("theId", "", "theTableName", new HashMap<String, Object>(), new ArrayList<UpdateRow>());
    }
    
    @Test
    public void constructorWithBlankSystemId() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("The system id can not be blank");
        
        new Update("theId", " ", "theTableName", new HashMap<String, Object>(), new ArrayList<UpdateRow>());
    }
    
    @Test
    public void constructorWithNullTableName() {
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("The table name can not be blank");
        
        new Update("theId", "theSystemId", null, new HashMap<String, Object>(), new ArrayList<UpdateRow>());
    }
    
    @Test
    public void constructorWithEmptyTableName() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("The table name can not be blank");
        
        new Update("theId", "theSystemId", "", new HashMap<String, Object>(), new ArrayList<UpdateRow>());
    }
    
    @Test
    public void constructorWithBlankTableName() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("The table name can not be blank");
        
        new Update("theId", "theSystemId", " ", new HashMap<String, Object>(), new ArrayList<UpdateRow>());
    }
    
    @Test
    public void constructorWithNullParameters() {
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("The parameters can not be null");
        
        new Update("theId", "theSystemId", "theTableName", null, new ArrayList<UpdateRow>());
    }
    
    @Test
    public void constructorWithNullRows() {
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("The rows can not be null");
        
        new Update("theId", "theSystemId", "theTableName", new HashMap<String, Object>(), null);
    }
    
    @Test
    public void getIdShouldReturnCorrectId() {
        Update update = new Update("theId", "theSystemId", "theTableName", new HashMap<String, Object>(), new ArrayList<UpdateRow>());
        assertEquals("theId", update.getId());
    }
    
    @Test
    public void getSystemIdShouldReturnCorrectSystemId(){
        Update update = new Update("theId", "theSystemId", "theTableName", new HashMap<String, Object>(), new ArrayList<UpdateRow>());
        assertEquals("theSystemId", update.getSystemId());
    }
    
    @Test
    public void getTableNameShouldReturnCorrectTableName(){
        Update update = new Update("theId", "theSystemId", "theTableName", new HashMap<String, Object>(), new ArrayList<UpdateRow>());
        assertEquals("theTableName", update.getTableName());
    }
    
    @Test
    public void getParameterNamesShouldReturnCorrectParameterNames() {
        Map<String, Object> theParameters = new HashMap<String, Object>();
        theParameters.put("a key", "a value");
        
        Update update = new Update("theId", "theSystemId", "theTableName", theParameters, new ArrayList<UpdateRow>());
        
        assertEquals(theParameters.keySet().size(), update.getParameterNames().size());
        for(String name : update.getParameterNames()) {
            assertTrue(theParameters.containsKey(name));
        }
    }
    
    @Test
    public void getParameterValueWithValidParameter() {
        Map<String, Object> theParameters = new HashMap<String, Object>();
        theParameters.put("a key", "a value");
        
        Update update = new Update("theId", "theSystemId", "theTableName", theParameters, new ArrayList<UpdateRow>());
        
        assertEquals(theParameters.get("a key"), update.getParameterValue("a key"));
    }
    
    @Test
    public void getParameterValueWithNullParameter() {
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("The parameter name cannot be blank.");
        
        Update update = new Update("theId", "theSystemId", "theTableName", new HashMap<String, Object>(), new ArrayList<UpdateRow>());
        update.getParameterValue(null);
    }
 
    @Test
    public void getParameterQuantityShouldReturnCorrectQuantity() {
        Map<String, Object> theParameters = new HashMap<String, Object>();
        theParameters.put("a key", "a value");
        
        Update update = new Update("theId", "theSystemId", "theTableName", theParameters, new ArrayList<UpdateRow>());
        
        assertEquals(theParameters.size(), update.getParametersQuantity());
    }
    
    @Test
    public void getParametersShouldReturnCorrectParameters() {
        Map<String, Object> theParameters = new HashMap<String, Object>();
        theParameters.put("a key", "a value");
        theParameters.put("a second key", "a second value");
        
        Update update = new Update("theId", "theSystemId", "theTableName", theParameters, new ArrayList<UpdateRow>());
        
        assertEquals(theParameters, update.getParameters());
    }
    
    @Test
    public void getRowsShouldReturnCorrectRows() {
        UpdateRow updateRow = new UpdateRow("update-row-id", new HashMap<String, Object>(), new HashMap<String, Object>());
        Collection<UpdateRow> theRows = new ArrayList<UpdateRow>();
        theRows.add(updateRow);
        Update update = new Update("update-id", "theSystemId", "theTableName", new HashMap<String, Object>(), theRows);
        assertEquals(theRows.size(), update.getRows().size());
        assertTrue(update.getRows().containsAll(theRows));
    }
    
    @Test
    public void getRowsIdShouldReturnCorrectRowIds() {
        UpdateRow updateRow = new UpdateRow("row-id", new HashMap<String, Object>(), new HashMap<String, Object>());
        Update update = new Update("update-id", "theSystemId", "theTableName", new HashMap<String, Object>(), Arrays.asList(updateRow));
        
        assertEquals("Wrong quantity of row ids after update creation.", update.getRowsId().size(), 1);
        assertTrue("Update row id not contained in the created update.", update.getRowsId().contains(updateRow.getId()));
    }
    
    @Test
    public void getRowShouldReturnCorrectRow() {
        UpdateRow updateRow = new UpdateRow("row-id", new HashMap<String, Object>(), new HashMap<String, Object>());
        Update update = new Update("update-id", "theSystemId", "theTableName", new HashMap<String, Object>(), Arrays.asList(updateRow));
        assertEquals(updateRow, update.getRow("row-id"));
    }
    
    @Test
    public void getRowWithNullId() {
        UpdateRow updateRow = new UpdateRow("row-id", new HashMap<String, Object>(), new HashMap<String, Object>());
        Update update = new Update("update-id", "theSystemId", "theTableName", new HashMap<String, Object>(), Arrays.asList(updateRow));
        
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("The id can not be blank");
        
        update.getRow(null);
    }
    
    @Test
    public void equalsShouldReturnTrueWhenComparedToAnotherIdenticalUpdate() {
        Update updateLeft = new Update("update-id", "theSystemId", "theTableName", new HashMap<String, Object>(), new ArrayList<UpdateRow>());
        Update updateRight = new Update("update-id", "theSystemId", "theTableName", new HashMap<String, Object>(), new ArrayList<UpdateRow>());
        
        assertEquals(updateLeft, updateRight);
    }
    
    @Test
    public void equalsShouldReturnFalseWhenIdIsNotEqual() {
        Update updateLeft = new Update("update-id-left", "theSystemId", "theTableName", new HashMap<String, Object>(), new ArrayList<UpdateRow>());
        Update updateRight = new Update("update-id-right", "theSystemId", "theTableName", new HashMap<String, Object>(), new ArrayList<UpdateRow>());
        
        assertNotEquals(updateLeft, updateRight);
    }
    
    @Test
    public void equalsShouldReturnFalseWhenSystemIdIsNotEqual() {
        Update updateLeft = new Update("update-id", "theSystemId-left", "theTableName", new HashMap<String, Object>(), new ArrayList<UpdateRow>());
        Update updateRight = new Update("update-id", "theSystemId-right", "theTableName", new HashMap<String, Object>(), new ArrayList<UpdateRow>());
        
        assertNotEquals(updateLeft, updateRight);
    }
    
    @Test
    public void equalsShouldReturnFalseWhenTableNameIsNotEqual() {
        Update updateLeft = new Update("update-id", "theSystemId", "theTableName-left", new HashMap<String, Object>(), new ArrayList<UpdateRow>());
        Update updateRight = new Update("update-id", "theSystemId", "theTableName-right", new HashMap<String, Object>(), new ArrayList<UpdateRow>());
        
        assertNotEquals(updateLeft, updateRight);
    }
    
    @Test
    public void equalsShouldReturnFalseWhenParametersAreNotEqual() {
        Map<String, Object> parametersLeft = new HashMap<String, Object>();
        parametersLeft.put("a key", "a value");
        Update updateLeft = new Update("update-id", "theSystemId", "theTableName", parametersLeft, new ArrayList<UpdateRow>());
        Update updateRight = new Update("update-id", "theSystemId", "theTableName", new HashMap<String, Object>(), new ArrayList<UpdateRow>());
        
        assertNotEquals(updateLeft, updateRight);
    }
    
    @Test
    public void equalsShouldReturnFalseWhenRowsAreNotEqual() {
        Update updateLeft = new Update("update-id", "theSystemId", "theTableName", new HashMap<String, Object>(), 
            Arrays.asList(new UpdateRow("updateRow-id", new HashMap<String, Object>(), new HashMap<String, Object>())));
        Update updateRight = new Update("update-id", "theSystemId", "theTableName", new HashMap<String, Object>(), new ArrayList<UpdateRow>());
        
        assertNotEquals(updateLeft, updateRight);
    }
    
    @Test
    public void toStringShouldReturnCorrectRepresentation() {
        assertEquals(
            "Update{id=update-id, systemId=theSystemId, tableName=theTableName, parameters={}, rows={}}",        
            new Update("update-id", "theSystemId", "theTableName", new HashMap<String, Object>(), new ArrayList<UpdateRow>()).toString());
    }
    
    @Test
    public void hashcodeShouldBeEqualForIdenticalUpdates() {
        Update updateLeft = new Update("update-id", "theSystemId", "theTableName", new HashMap<String, Object>(), new ArrayList<UpdateRow>());
        Update updateRight = new Update("update-id", "theSystemId", "theTableName", new HashMap<String, Object>(), new ArrayList<UpdateRow>());
        
        assertEquals(updateLeft.hashCode(), updateRight.hashCode());
    }
}
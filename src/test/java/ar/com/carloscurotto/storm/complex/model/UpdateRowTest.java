package ar.com.carloscurotto.storm.complex.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class UpdateRowTest {
    
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    
    @Test
    public void constructorWithValidParameters() {
        String theId = "theId";
        Map<String, Object> theKeyColumns = new HashMap<String, Object>();
        theKeyColumns.put("a key", "a value");
        Map<String, Object> theUpdateColumns = new HashMap<String, Object>();
        theUpdateColumns.put("a key", "a value");
        
        UpdateRow updateRow = new UpdateRow(theId, 1l, theKeyColumns, theUpdateColumns);
        
        assertEquals(theId, updateRow.getId());
        assertEquals("a value", updateRow.getKeyColumnValue("a key"));
        assertEquals(theKeyColumns.entrySet().size(), updateRow.getKeyColumnEntries().size());
        
        assertEquals("a value", updateRow.getUpdateColumnValue("a key"));
        assertEquals(theUpdateColumns.entrySet().size(), updateRow.getUpdateColumnEntries().size());
    }
    
    @Test
    public void constructorWithEmptyMaps() {
        String theId = "theId";
        Map<String, Object> theKeyColumns = new HashMap<String, Object>();
        Map<String, Object> theUpdateColumns = new HashMap<String, Object>();
        
        UpdateRow updateRow = new UpdateRow(theId, 1l, theKeyColumns, theUpdateColumns);
        
        assertEquals(theId, updateRow.getId());
        assertEquals(null, updateRow.getKeyColumnValue("a key"));
        assertEquals(theKeyColumns.entrySet().size(), updateRow.getKeyColumnEntries().size());
        
        assertEquals(null, updateRow.getUpdateColumnValue("a key"));
        assertEquals(theUpdateColumns.entrySet().size(), updateRow.getUpdateColumnEntries().size());
    }
    
    @Test
    public void constructorWithNullId(){
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("The id can not be blank");
        new UpdateRow(null, 1l, new HashMap<String, Object>(), new HashMap<String, Object>());
    }
    
    @Test
    public void constructorWithEmptyId(){
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("The id can not be blank");
        new UpdateRow("", 1l, new HashMap<String, Object>(), new HashMap<String, Object>());
    }
    
    @Test
    public void constructorWithBlankId(){
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("The id can not be blank");
        new UpdateRow(" ", 1l, new HashMap<String, Object>(), new HashMap<String, Object>());
    }
    
    @Test
    public void constructorWithTimestampMinorThanZero() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("The timestamp must be greater or equal to 0.");
        new UpdateRow("theId", -1l, new HashMap<String, Object>(), new HashMap<String, Object>());
    }
    
    @Test
    public void constructorWithNullKeyColumns() {
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("The key columns cannot be null.");
        new UpdateRow("theId", 1l, null, new HashMap<String, Object>());
    }
    
    @Test
    public void constructorWithNullUpdateColumns() {
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("The update columns cannot be null.");
        new UpdateRow("theId", 1l, new HashMap<String, Object>(), null);
    }
    
    @Test
    public void getIdShouldReturnIdPassedOnConstructor() {
        UpdateRow updateRow = new UpdateRow("theId", 1l, new HashMap<String, Object>(), new HashMap<String, Object>());
        assertEquals("theId", updateRow.getId());
    }
    
    @Test
    public void getKeyColumnEntriesShouldReturnEntrySetWithCorrectSize(){
        Map<String, Object> keyColumns = new HashMap<String, Object>();
        keyColumns.put("a key", "a value");
        UpdateRow updateRow = new UpdateRow("theId", 1l, keyColumns, new HashMap<String, Object>());
        
        assertEquals(keyColumns.entrySet().size(), updateRow.getKeyColumnEntries().size());
    }
    
    @Test
    public void getKeyColumnEntriesShouldReturnCorrectEntrySet(){
        Map<String, Object> keyColumns = new HashMap<String, Object>();
        keyColumns.put("a key", "a value");
        UpdateRow updateRow = new UpdateRow("theId", 1l, keyColumns, new HashMap<String, Object>());
        
        Entry<String, Object> entry = updateRow.getKeyColumnEntries().iterator().next();
        assertEquals(keyColumns.get(entry.getKey()), entry.getValue());
        
    }
    
    @Test
    public void getKeyColumnNamesShouldReturnKeyCollectionWithCorrectSize() {
        Map<String, Object> keyColumns = new HashMap<String, Object>();
        keyColumns.put("a key", "a value");
        UpdateRow updateRow = new UpdateRow("theId", 1l, keyColumns, new HashMap<String, Object>());
        
        assertEquals(keyColumns.keySet().size(), updateRow.getKeyColumnNames().size());
    }
    
    @Test
    public void getKeyColumnNamesShouldReturnCorrectKeyCollection() {
        Map<String, Object> keyColumns = new HashMap<String, Object>();
        keyColumns.put("a key", "a value");
        UpdateRow updateRow = new UpdateRow("theId", 1l, keyColumns, new HashMap<String, Object>());
        assertTrue(updateRow.getKeyColumnNames().containsAll(keyColumns.keySet()));
    }
    
    @Test
    public void getKeyColumnValueShouldReturnCorrectValue() {
        Map<String, Object> keyColumns = new HashMap<String, Object>();
        keyColumns.put("a key", "a value");
        UpdateRow updateRow = new UpdateRow("theId", 1l, keyColumns, new HashMap<String, Object>());
        assertEquals(keyColumns.get("a key"), updateRow.getKeyColumnValue("a key"));
    }
    
    @Test
    public void getKeyColumnValueShouldWithNullKey() {
        UpdateRow updateRow = new UpdateRow("theId", 1l, new HashMap<String, Object>(), new HashMap<String, Object>());
        
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("The key column name cannot be blank.");
        
        updateRow.getKeyColumnValue(null);
    }
    
    @Test
    public void getKeyColumnValueShouldWithEmptyKey() {
        UpdateRow updateRow = new UpdateRow("theId", 1l, new HashMap<String, Object>(), new HashMap<String, Object>());
        
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("The key column name cannot be blank.");
        
        updateRow.getKeyColumnValue("");
    }
    
    @Test
    public void getKeyColumnValueShouldWithBlankKey() {
        UpdateRow updateRow = new UpdateRow("theId", 1l, new HashMap<String, Object>(), new HashMap<String, Object>());
        
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("The key column name cannot be blank.");
        
        updateRow.getKeyColumnValue(" ");
    }
    
    @Test
    public void getUpdateColumnEntriesShouldReturnEntrySetWithCorrectSize(){
        Map<String, Object> updateColumns = new HashMap<String, Object>();
        updateColumns.put("a key", "a value");
        UpdateRow updateRow = new UpdateRow("theId", 1l, new HashMap<String, Object>(), updateColumns);
        
        assertEquals(updateColumns.entrySet().size(), updateRow.getUpdateColumnEntries().size());
    }
    
    @Test
    public void getUpdateColumnEntriesShouldReturnCorrectEntrySet(){
        Map<String, Object> updateColumns = new HashMap<String, Object>();
        updateColumns.put("a key", "a value");
        UpdateRow updateRow = new UpdateRow("theId", 1l, new HashMap<String, Object>(), updateColumns);
        
        Entry<String, Object> entry = updateRow.getUpdateColumnEntries().iterator().next();
        assertEquals(updateColumns.get(entry.getKey()), entry.getValue());
        
    }
    
    @Test
    public void getUpdateColumnNamesShouldReturnKeyCollectionWithCorrectSize() {
        Map<String, Object> updateColumns = new HashMap<String, Object>();
        updateColumns.put("a key", "a value");
        UpdateRow updateRow = new UpdateRow("theId", 1l, new HashMap<String, Object>(), updateColumns);
        
        assertEquals(updateColumns.keySet().size(), updateRow.getUpdateColumnNames().size());
    }
    
    @Test
    public void getUpdateColumnNamesShouldReturnCorrectKeyCollection() {
        Map<String, Object> updateColumns = new HashMap<String, Object>();
        updateColumns.put("a key", "a value");
        UpdateRow updateRow = new UpdateRow("theId", 1l, new HashMap<String, Object>(), updateColumns);
        
        assertTrue(updateRow.getUpdateColumnNames().containsAll(updateColumns.keySet()));
     }
    
    @Test
    public void getUpdateColumnValueShouldReturnCorrectValue() {
        Map<String, Object> updateColumns = new HashMap<String, Object>();
        updateColumns.put("a key", "a value");
        UpdateRow updateRow = new UpdateRow("theId", 1l, new HashMap<String, Object>(), updateColumns);
        
        assertEquals(updateColumns.get("a key"), updateRow.getUpdateColumnValue("a key"));
    }
    
    @Test
    public void getUpdateColumnValueShouldWithNullKey() {
        UpdateRow updateRow = new UpdateRow("theId", 1l, new HashMap<String, Object>(), new HashMap<String, Object>());
        
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("The update column name cannot be blank.");
        
        updateRow.getUpdateColumnValue(null);
    }
    
    @Test
    public void getUpdateColumnValueShouldWithEmptyKey() {
        UpdateRow updateRow = new UpdateRow("theId", 1l, new HashMap<String, Object>(), new HashMap<String, Object>());
        
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("The update column name cannot be blank.");
        
        updateRow.getUpdateColumnValue("");
    }
    
    @Test
    public void getUpdateColumnValueShouldWithBlankKey() {
        UpdateRow updateRow = new UpdateRow("theId", 1l, new HashMap<String, Object>(), new HashMap<String, Object>());
        
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("The update column name cannot be blank.");
        
        updateRow.getUpdateColumnValue(" ");
    }
    
    @Test
    public void equalsWithNullParameter(){
        UpdateRow updateRowLeft = new UpdateRow("theId", 1l, new HashMap<String, Object>(), new HashMap<String, Object>());
        assertFalse(updateRowLeft.equals(null));
    }
    
    @Test
    public void equalsWithParameterOfTypeOtherThanUpdateRow(){
        UpdateRow updateRowLeft = new UpdateRow("theId", 1l, new HashMap<String, Object>(), new HashMap<String, Object>());
        assertFalse(updateRowLeft.equals(new Object()));
    }
    
    @Test
    public void equalsShouldReturnTrueWhenComparedToAnotherIdenticalUpdateRow(){
        UpdateRow updateRowLeft = new UpdateRow("theId", 1l, new HashMap<String, Object>(), new HashMap<String, Object>());
        UpdateRow updateRowRight = new UpdateRow("theId", 1l, new HashMap<String, Object>(), new HashMap<String, Object>());
        assertTrue(updateRowLeft.equals(updateRowRight));
    }
    
    @Test
    public void equalsShouldReturnFalseWhenIdIsNotEqual(){
        UpdateRow updateRowLeft = new UpdateRow("theIdLeft", 1l, new HashMap<String, Object>(), new HashMap<String, Object>());
        UpdateRow updateRowRight = new UpdateRow("theIdRight", 1l, new HashMap<String, Object>(), new HashMap<String, Object>());
        assertFalse(updateRowLeft.equals(updateRowRight));
    }
    
    @Test
    public void equalsShouldReturnFalseWhenTimestampIsNotEqual(){
        UpdateRow updateRowLeft = new UpdateRow("theId", 1l, new HashMap<String, Object>(), new HashMap<String, Object>());
        UpdateRow updateRowRight = new UpdateRow("theId", 2l, new HashMap<String, Object>(), new HashMap<String, Object>());
        assertFalse(updateRowLeft.equals(updateRowRight));
    }
    
    @Test
    public void equalsShouldReturnFalseWhenKeyColumnsIsNotEqual(){
        Map<String, Object> keyColumnsLeft = new HashMap<String, Object>();
        keyColumnsLeft.put("left key", "left value");
        
        Map<String, Object> keyColumnsRight = new HashMap<String, Object>();
        keyColumnsRight.put("right key", "right value");
        
        UpdateRow updateRowLeft = new UpdateRow("theId", 1l, keyColumnsLeft , new HashMap<String, Object>());
        UpdateRow updateRowRight = new UpdateRow("theId", 1l, keyColumnsRight, new HashMap<String, Object>());
        assertFalse(updateRowLeft.equals(updateRowRight));
    }
    
    @Test
    public void equalsShouldReturnFalseWhenUpdateColumnsIsNotEqual(){
        Map<String, Object> updateColumnsLeft = new HashMap<String, Object>();
        updateColumnsLeft.put("left key", "left value");
        
        Map<String, Object> updateColumnsRight = new HashMap<String, Object>();
        updateColumnsRight.put("right key", "right value");
        
        UpdateRow updateRowLeft = new UpdateRow("theId", 1l, new HashMap<String, Object>(), updateColumnsLeft);
        UpdateRow updateRowRight = new UpdateRow("theId", 1l, new HashMap<String, Object>(), updateColumnsRight);
        assertFalse(updateRowLeft.equals(updateRowRight));
    }
    
    @Test
    public void hashCodeShouldBeEqualWhenBothObjectsAreEqual(){
        UpdateRow updateRowLeft = new UpdateRow("theId", 1l, new HashMap<String, Object>(), new HashMap<String, Object>());
        UpdateRow updateRowRight = new UpdateRow("theId", 1l, new HashMap<String, Object>(), new HashMap<String, Object>());
        assertTrue(updateRowLeft.equals(updateRowRight));
        assertEquals(updateRowLeft.hashCode(), updateRowRight.hashCode());
    }
    
    @Test
    public void toStringShouldReturnASpecificString(){
        UpdateRow updateRow = new UpdateRow("theId", 1l, new HashMap<String, Object>(), new HashMap<String, Object>());
        assertEquals("UpdateRow{id=theId, keyColumns={}, updateColumns={}}", updateRow.toString());
    }
}

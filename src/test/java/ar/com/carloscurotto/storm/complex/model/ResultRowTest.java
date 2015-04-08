package ar.com.carloscurotto.storm.complex.model;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertFalse;

import org.apache.commons.lang3.StringUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ResultRowTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void creationWithNullId() {
        thrown.expect(NullPointerException.class);
        thrown.expectMessage(equalTo("The id can not be blank."));
        ResultRow.create(null, ResultStatus.SUCCESS, "test-message");
    }

    @Test
    public void creationWithEmptyId() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("The id can not be blank."));
        ResultRow.create(StringUtils.EMPTY, ResultStatus.SUCCESS, "test-message");
    }
    
    @Test
    public void creationWithBlankId() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("The id can not be blank."));
        ResultRow.create(StringUtils.SPACE, ResultStatus.SUCCESS, "test-message");
    }

    @Test
    public void creationWithNullStatus() {
        thrown.expect(NullPointerException.class);
        thrown.expectMessage(equalTo("The status can not be null."));
        ResultRow.create("test-id", null, "test-message");
    }

    @Test
    public void creationWithValidIdAndValidStatusAndAMessage(){
        ResultRow resultRow = ResultRow.create("test-id", ResultStatus.SUCCESS, "test-message");
        assertEquals("test-id", resultRow.getId());
        assertTrue(resultRow.isSuccessful());
        assertEquals("test-message", resultRow.getMessage());
    }
    
    @Test
    public void skipShouldReturnASkipStatus() {
        ResultRow resultRow = ResultRow.skip("theId");
        assertEquals("theId", resultRow.getId());
        assertNull(resultRow.getMessage());
        assertTrue(resultRow.isSkipped());
    }
    
    @Test
    public void successShouldReturnASuccessStatus() {
        ResultRow resultRow = ResultRow.success("theId", "theMessage");
        assertEquals("theId", resultRow.getId());
        assertTrue(resultRow.isSuccessful());
        assertEquals("theMessage", resultRow.getMessage());
    }
    
    @Test
    public void successWithNullMessage() {
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("The message can not be blank.");
        
        ResultRow.success("theId", null);
    }
    
    @Test
    public void successWithEmptyMessage() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("The message can not be blank.");
        
        ResultRow.success("theId", StringUtils.EMPTY);
    }
    
    @Test
    public void successWithBlankMessage() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("The message can not be blank.");
        
        ResultRow.success("theId", StringUtils.SPACE);
    }
    
    //--
    @Test
    public void failureShouldReturnASuccessStatus() {
        ResultRow resultRow = ResultRow.failure("theId", "theMessage");
        assertEquals("theId", resultRow.getId());
        assertTrue(resultRow.isFailure());
        assertEquals("theMessage", resultRow.getMessage());
    }
    
    @Test
    public void failureWithNullMessage() {
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("The message can not be blank.");
        
        ResultRow.failure("theId", null);
    }
    
    @Test
    public void failureWithEmptyMessage() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("The message can not be blank.");
        
        ResultRow.failure("theId", StringUtils.EMPTY);
    }
    
    @Test
    public void failureWithBlankMessage() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("The message can not be blank.");
        
        ResultRow.failure("theId", StringUtils.SPACE);
    }
    
    @Test
    public void getIdShouldReturnCorrectId(){
        ResultRow resultRow = ResultRow.create("theId", ResultStatus.SUCCESS, "theMessage");
        assertEquals("theId", resultRow.getId());
    }
    
    @Test
    public void getMessageShouldReturnCorrectMessage(){
        ResultRow resultRow = ResultRow.create("theId", ResultStatus.SUCCESS, "theMessage");
        assertEquals("theMessage", resultRow.getMessage());
    }
    
    @Test
    public void isSuccessfulShouldReturnTrueWhenSuccess(){
        ResultRow resultRow = ResultRow.create("theId", ResultStatus.SUCCESS, "theMessage");
        assertTrue(resultRow.isSuccessful());
        assertFalse(resultRow.isSkipped());
        assertFalse(resultRow.isFailure());
    }
    
    @Test
    public void isFailureShouldReturnTrueWhenFailure(){
        ResultRow resultRow = ResultRow.create("theId", ResultStatus.FAILURE, "theMessage");
        assertFalse(resultRow.isSuccessful());
        assertFalse(resultRow.isSkipped());
        assertTrue(resultRow.isFailure());
    }
    
    @Test
    public void isSkippedShouldReturnTrueWhenFailure(){
        ResultRow resultRow = ResultRow.create("theId", ResultStatus.SKIPPED, "theMessage");
        assertFalse(resultRow.isSuccessful());
        assertTrue(resultRow.isSkipped());
        assertFalse(resultRow.isFailure());
    }
    
    @Test
    public void equalsShouldReturnTrueWhenComparingEquivalentResultRows(){
        ResultRow resultRowLeft = ResultRow.create("theId", ResultStatus.SUCCESS, "theMessage");
        ResultRow resultRowRight = ResultRow.create("theId", ResultStatus.SUCCESS, "theMessage");
        assertEquals(resultRowLeft, resultRowRight);
    }
    
    @Test
    public void equalsShoudlReturnFalseWhenIdNotEquals(){
        ResultRow resultRowLeft = ResultRow.create("theId-left", ResultStatus.SUCCESS, "theMessage");
        ResultRow resultRowRight = ResultRow.create("theId-right", ResultStatus.SUCCESS, "theMessage");
        assertNotEquals(resultRowLeft, resultRowRight);
    }
    
    @Test
    public void equalsShouldReturnFalseWhenMessageNotEquals(){
        ResultRow resultRowLeft = ResultRow.create("theId", ResultStatus.SUCCESS, "theMessage-left");
        ResultRow resultRowRight = ResultRow.create("theId", ResultStatus.SUCCESS, "theMessage-right");
        assertNotEquals(resultRowLeft, resultRowRight);
    }
    
    @Test
    public void equalsShouldReturnFalseWhenStatusNotEquals() {
        ResultRow resultRowLeft = ResultRow.create("theId", ResultStatus.SUCCESS, "theMessage");
        ResultRow resultRowRight = ResultRow.create("theId", ResultStatus.FAILURE, "theMessage");
        assertNotEquals(resultRowLeft, resultRowRight);
    }
    
    @Test
    public void hashCodeShouldBeEqualsForEqualResultRows() {
        ResultRow resultRowLeft = ResultRow.create("theId", ResultStatus.SUCCESS, "theMessage");
        ResultRow resultRowRight = ResultRow.create("theId", ResultStatus.SUCCESS, "theMessage");
        assertEquals(resultRowLeft, resultRowRight);
        assertEquals(resultRowLeft.hashCode(), resultRowRight.hashCode());
    }       
}
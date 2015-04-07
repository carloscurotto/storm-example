package ar.com.carloscurotto.storm.complex.model;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;

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
    
    
}

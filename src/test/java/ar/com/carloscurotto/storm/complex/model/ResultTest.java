package ar.com.carloscurotto.storm.complex.model;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.apache.commons.lang3.StringUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ResultTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void constructionWithNullId() {
        thrown.expect(NullPointerException.class);
        thrown.expectMessage(equalTo("The id can not be blank."));
             
        new Result(null, Arrays.asList(ResultRow.create("theId", ResultStatus.SUCCESS, "theMessage")));
    }

    @Test
    public void constructionWithEmptyId() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("The id can not be blank."));
        new Result(StringUtils.EMPTY, Arrays.asList(ResultRow.create("theId", ResultStatus.SUCCESS, "theMessage")));
    }
    
    @Test
    public void constructionWithBlankId() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("The id can not be blank."));
        new Result(StringUtils.SPACE, Arrays.asList(ResultRow.create("theId", ResultStatus.SUCCESS, "theMessage")));
    }

    @Test
    public void constructionWithNullRows() {
        thrown.expect(NullPointerException.class);
        thrown.expectMessage(equalTo("The rows can not be null."));
        new Result("test-result-id", null);
    }

    @Test
    public void constructionWithEmptyRows() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("The rows can not be empty."));
        new Result("test-result-id", Collections.<ResultRow> emptyList());
    }

    @Test
    public void constructionWithValidIdAndValidRows() {
        final ResultRow row = ResultRow.success("test-row-id", "test-row-message");

        final String resultId = "test-result-id";
        Result result = new Result(resultId, Arrays.asList(new ResultRow[] {row}));

        assertThat("Null result created.", result, notNullValue());
        assertThat("Wrong result id after creation.", result.getId(), equalTo(resultId));
        assertThat("Null result rows after creation.", result.getRows(), notNullValue());
        assertThat("Null result rows quantity after creation.", result.getRows().size(), equalTo(1));
        assertThat("Wrong result rows after creation.", result.getRows().iterator().next(), equalTo(row));
    }
    
    @Test
    public void getIdShouldReturnCorrectId() {
        Result result = new Result("theId", Arrays.asList(ResultRow.create("theId", ResultStatus.SUCCESS, "theMessage")));
        assertEquals("theId", result.getId());
    }
    
    @Test
    public void getRowsShouldReturnCorrectRows() {
        ResultRow resultRow = ResultRow.create("theId", ResultStatus.SUCCESS, "theMessage");
        Collection<ResultRow> rows = new ArrayList<ResultRow>();
        rows.add(resultRow);
        
        Result result = new Result("theId", rows);
        assertEquals(rows.size(), result.getRows().size());
        assertTrue(result.getRows().containsAll(rows));
    }

    @Test
    public void getRowWithNullId() {
        thrown.expect(NullPointerException.class);
        thrown.expectMessage(equalTo("The id can not be blank."));
        Result result = new Result("test-result-id", Arrays.asList(new ResultRow[] { ResultRow
                .success("test-row-id", "test-row-message") }));
        result.getRow(null);
    }

    @Test
    public void getRowWithEmptyId() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("The id can not be blank."));
        Result result = new Result("test-result-id", Arrays.asList(new ResultRow[] { ResultRow
                .success("test-row-id", "test-row-message") }));
        result.getRow(StringUtils.EMPTY);
    }
    
    @Test
    public void getRowWithBlankId() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("The id can not be blank."));
        Result result = new Result("test-result-id", Arrays.asList(new ResultRow[] { ResultRow
                .success("test-row-id", "test-row-message") }));
        result.getRow(StringUtils.SPACE);
    }

    @Test
    public void getRowWithNonExistentId() {
        final String nonExistentRowId = "non-existent-row-id";
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage(equalTo("Can not find a row with id [" + nonExistentRowId + "]."));
        Result result = new Result("test-result-id", Arrays.asList(new ResultRow[] { ResultRow
                .success("test-row-id", "test-row-message") }));
        result.getRow(nonExistentRowId);
    }

    @Test
    public void equalityWithEqualResults() {
        Result first = new Result("test-result-id", Arrays.asList(new ResultRow[] { ResultRow
                .success("test-row-id", "test-row-message") }));
        Result second = new Result("test-result-id", Arrays.asList(new ResultRow[] { ResultRow
                .success("test-row-id", "test-row-message") }));
        assertThat("Equal objects are not equivalent.", first, equalTo(second));
        assertThat("Equal objects do not have same hashcode.", first.hashCode(), equalTo(second.hashCode()));
    }

    @Test
    public void equalityWithNonEqualIds() {
        Result first = new Result("first-result-id", Arrays.asList(new ResultRow[] { ResultRow
                .success("test-row-id", "test-row-message") }));
        Result second = new Result("second-result-id", Arrays.asList(new ResultRow[] { ResultRow
                .success("test-row-id", "test-row-message") }));
        assertThat("Non equal objects are equivalent.", first, not(equalTo(second)));
    }

    @Test
    public void equalityWithNonEqualRows() {
        Result first = new Result("result-id", Arrays.asList(new ResultRow[] { ResultRow
                .success("test-row-id-first", "test-row-message") }));
        Result second = new Result("result-id", Arrays.asList(new ResultRow[] { ResultRow
                .success("test-row-id-second", "test-row-message") }));
        assertThat("Non equal objects are equivalent.", first, not(equalTo(second)));
    }
    
    @Test
    public void toStringShouldReturnCorrectStringRepresentation() {
        Result r = new Result("result-id", Arrays.asList(new ResultRow[] { ResultRow
                .success("test-row-id-second", "test-row-message") }));
        assertEquals(
            "Result{id=result-id, rows={test-row-id-second=ResultRow{id=test-row-id-second, message=test-row-message, status=SUCCESS}}}",
            r.toString());
    }
    
    @Test
    public void hashCodeShouldBeEqualForEqualResults() {
        Result first = new Result("test-result-id", Arrays.asList(new ResultRow[] { ResultRow
                .success("test-row-id", "test-row-message") }));
        Result second = new Result("test-result-id", Arrays.asList(new ResultRow[] { ResultRow
                .success("test-row-id", "test-row-message") }));
        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
    }

}

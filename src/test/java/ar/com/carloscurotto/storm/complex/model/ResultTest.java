package ar.com.carloscurotto.storm.complex.model;

import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
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
        new Result(null, Collections.<ResultRow> emptyList());
    }

    @Test
    public void constructionWithEmptyId() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("The id can not be blank."));
        new Result(StringUtils.EMPTY, Collections.<ResultRow> emptyList());
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
    public void constructionWithValidIdAndRows() {
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
    public void equalityWithNonEqualResults() {
        Result first = new Result("first-result-id", Arrays.asList(new ResultRow[] { ResultRow
                .success("test-row-id", "test-row-message") }));
        Result second = new Result("second-result-id", Arrays.asList(new ResultRow[] { ResultRow
                .success("test-row-id", "test-row-message") }));
        assertThat("Non equal objects are equivalent.", first, not(equalTo(second)));
    }

}

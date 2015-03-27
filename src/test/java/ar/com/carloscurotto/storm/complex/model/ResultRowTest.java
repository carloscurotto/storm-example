package ar.com.carloscurotto.storm.complex.model;

import static org.hamcrest.Matchers.equalTo;

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
    public void creationWithNullStatus() {
        thrown.expect(NullPointerException.class);
        thrown.expectMessage(equalTo("The status can not be null."));
        ResultRow.create("test-id", null, "test-message");
    }

}

package pl.allegro.tech.search.elasticsearch.tools.reindex.process;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;

import java.util.Collection;

public class BulkResultAssert extends AbstractAssert<BulkResultAssert, BulkResult> {

  protected BulkResultAssert(BulkResult actual) {
    super(actual, BulkResultAssert.class);
  }

  public static BulkResultAssert assertThat(BulkResult actual) {
    return new BulkResultAssert(actual);
  }

  public BulkResultAssert hasFailedCount(long failedCount) {
    isNotNull();
    if (actual.getFailedCount() != failedCount) {
      failWithMessage("Expected failedCount to be <%d> but was <%d>", failedCount, actual.getFailedCount());
    }
    return this;
  }

  public BulkResultAssert hasFailedIds(Collection<String> failedIds) {
    isNotNull();
    Assertions.assertThat(failedIds).hasSameElementsAs(actual.getFailedIds());
    return this;
  }


}
package pl.allegro.tech.search.elasticsearch.tools.reindex.statistics;

import org.assertj.core.api.AbstractAssert;

public class ReindexingSummaryAssert extends AbstractAssert<ReindexingSummaryAssert, ReindexingSummary> {

  protected ReindexingSummaryAssert(ReindexingSummary actual) {
    super(actual, ReindexingSummaryAssert.class);
  }

  public static ReindexingSummaryAssert assertThat(ReindexingSummary actual) {
    return new ReindexingSummaryAssert(actual);
  }

  public ReindexingSummaryAssert hasIndexedCount(long indexedCount) {
    isNotNull();
    if (actual.getIndexed() != indexedCount) {
      failWithMessage("Expected indexedCount to be <%d> but was <%d>", indexedCount, actual.getIndexed());
    }
    return this;
  }

  public ReindexingSummaryAssert hasQueriedCount(long queriedCount) {
    isNotNull();
    if (actual.getQueried() != queriedCount) {
      failWithMessage("Expected queriedCount to be <%d> but was <%d>", queriedCount, actual.getQueried());
    }
    return this;
  }

  public ReindexingSummaryAssert hasFailedIndexedCount(long failedIndexedCount) {
    isNotNull();
    if (actual.getFailedIndexed() != failedIndexedCount) {
      failWithMessage("Expected failedIndexedCount to be <%d> but was <%d>", failedIndexedCount, actual.getFailedIndexed());
    }
    return this;
  }

}
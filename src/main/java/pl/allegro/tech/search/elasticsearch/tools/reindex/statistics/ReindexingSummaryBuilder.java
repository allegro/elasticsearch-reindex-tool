package pl.allegro.tech.search.elasticsearch.tools.reindex.statistics;

public final class ReindexingSummaryBuilder {
  private long queried;
  private long indexed;
  private long failedIndexed;

  private ReindexingSummaryBuilder() {
  }

  public ReindexingSummaryBuilder setQueried(long queried) {
    this.queried = queried;
    return this;
  }

  public ReindexingSummaryBuilder setIndexed(long indexed) {
    this.indexed = indexed;
    return this;
  }

  public ReindexingSummaryBuilder setFailedIndexed(long failedIndexed) {
    this.failedIndexed = failedIndexed;
    return this;
  }

  public ReindexingSummary createReindexingSummary() {
    return new ReindexingSummary(queried, indexed, failedIndexed);
  }

  public static ReindexingSummaryBuilder builder() {
    return new ReindexingSummaryBuilder();
  }
}
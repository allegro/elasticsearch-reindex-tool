package pl.allegro.tech.search.elasticsearch.tools.reindex.statistics;

public class ReindexingSummary {
  private final long queried;
  private final long indexed;
  private final long failedIndexed;

  ReindexingSummary(long queried, long indexed, long failedIndexed) {
    this.queried = queried;
    this.indexed = indexed;
    this.failedIndexed = failedIndexed;
  }

  public long getQueried() {
    return queried;
  }

  public long getIndexed() {
    return indexed;
  }

  public long getFailedIndexed() {
    return failedIndexed;
  }
}

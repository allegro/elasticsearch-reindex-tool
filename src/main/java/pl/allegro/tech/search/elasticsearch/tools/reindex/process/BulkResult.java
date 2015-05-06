package pl.allegro.tech.search.elasticsearch.tools.reindex.process;

import java.util.Collection;
import java.util.Collections;

public class BulkResult {

  private final int indexedCount;

  private final Collection<String> failedIds;

  public BulkResult(int indexedCount, Collection<String> failedIds) {
    this.indexedCount = indexedCount;
    this.failedIds = Collections.unmodifiableCollection(failedIds);
  }

  public int getIndexedCount() {
    return indexedCount;
  }

  public long getFailedCount() {
    return failedIds.size();
  }

  public Collection<String> getFailedIds() {
    return failedIds;
  }
}

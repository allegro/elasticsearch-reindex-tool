package pl.allegro.tech.search.elasticsearch.tools.reindex.process;

import org.elasticsearch.action.search.SearchResponse;

public class QueryProcess implements Runnable {

  private final ProcessSynchronizer processSynchronizer;
  private final QueryComponent queryComponent;

  public QueryProcess(ProcessSynchronizer processSynchronizer, QueryComponent queryComponent) {
    this.processSynchronizer = processSynchronizer;
    this.queryComponent = queryComponent;
  }

  @Override
  public void run() {
    try {
      SearchResponse response = queryComponent.prepareSearchScrollRequest();

      if (queryComponent.searchResultsNotEmpty(response)) {
        while (true) {
          if (processSynchronizer.tryFillQueueWithSearchHits(response)) {
            response = queryComponent.getNextScrolledSearchResults(response.getScrollId());
            if (queryComponent.getResponseSize(response) == 0) {
              break;
            }
          }
        }
      }
    } catch (final Exception e) {
      processSynchronizer.addProcessingException(e);
    }
    processSynchronizer.subtractWorkingQueryProcess();
  }

}

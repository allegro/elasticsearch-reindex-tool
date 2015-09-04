package pl.allegro.tech.search.elasticsearch.tools.reindex.process;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class QueryProcessTest {

  @Test
  public void shouldNotFillQueueWhenQueryResultEmpty() throws Exception {
    //given
    SearchResponse searchResponse = new SearchResponse();
    ProcessSynchronizer processSynchronizer = mock(ProcessSynchronizer.class);
    QueryComponent queryComponent = mock(QueryComponent.class);
    when(queryComponent.prepareSearchScrollRequest()).thenReturn(searchResponse);

    //when
    new QueryProcess(processSynchronizer, queryComponent).run();

    //then
    verify(processSynchronizer, never()).tryFillQueueWithSearchHits(searchResponse);
    verify(processSynchronizer).subtractWorkingQueryProcess();
  }

  @Test
  public void shouldFillQueueWhenQueryResultNotEmptyInOneChunk() throws Exception {
    //given
    SearchResponse searchResponse = new SearchResponse();
    ProcessSynchronizer processSynchronizer = createProcessSynchronizerMock();
    QueryComponent queryComponent = mock(QueryComponent.class);
    when(queryComponent.prepareSearchScrollRequest()).thenReturn(searchResponse);
    when(queryComponent.searchResultsNotEmpty(searchResponse)).thenReturn(true);

    //when
    new QueryProcess(processSynchronizer, queryComponent).run();

    //then
    verify(processSynchronizer, times(1)).tryFillQueueWithSearchHits(searchResponse);
    verify(processSynchronizer).subtractWorkingQueryProcess();
  }

  @Test
  public void shouldFillQueueWhenQueryResultNotEmptyInTwoChunks() throws Exception {
    //given
    SearchResponse searchResponse = createSearchResponseWithScrollId("scrollId");
    ProcessSynchronizer processSynchronizer = createProcessSynchronizerMock();
    QueryComponent queryComponent = mock(QueryComponent.class);
    when(queryComponent.prepareSearchScrollRequest()).thenReturn(searchResponse);
    when(queryComponent.searchResultsNotEmpty(searchResponse)).thenReturn(true);
    when(queryComponent.getResponseSize(searchResponse)).thenReturn(1, 0);
    when(queryComponent.getNextScrolledSearchResults("scrollId")).thenReturn(searchResponse);

    //when
    new QueryProcess(processSynchronizer, queryComponent).run();

    //then
    verify(processSynchronizer, times(2)).tryFillQueueWithSearchHits(searchResponse);
    verify(processSynchronizer).subtractWorkingQueryProcess();
  }

  private SearchResponse createSearchResponseWithScrollId(String scrollId) {
    return new SearchResponse(InternalSearchResponse.empty(), scrollId, 1, 1, 1, new ShardSearchFailure[0]);
  }

  private ProcessSynchronizer createProcessSynchronizerMock() {
    ProcessSynchronizer processSynchronizer = mock(ProcessSynchronizer.class);
    when(processSynchronizer.tryFillQueueWithSearchHits(any(SearchResponse.class))).thenReturn(true);
    return processSynchronizer;
  }


}
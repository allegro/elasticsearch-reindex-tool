package pl.allegro.tech.search.elasticsearch.tools.reindex.process;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.Test;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProcessSynchronizerTest {

  public static final int QUERY_SEGMENT_SIZE = 3;

  @Test
  public void shouldHasDataToBeIndexedBeTrueWhenWorkingQueueIsNotEmpty() throws Exception {
    //given
    ProcessSynchronizer processSynchronizer = new ProcessSynchronizer(QUERY_SEGMENT_SIZE);
    //when
    boolean result = processSynchronizer.hasDataToBeIndexed();
    //then
    assertTrue(result);
  }

  @Test
  public void shouldHasDataToBeIndexedBeTrueWhenDataQueueIsNotEmpty() throws Exception {
    //given
    ProcessSynchronizer processSynchronizer = new ProcessSynchronizer(QUERY_SEGMENT_SIZE);
    setupDataQueueForProcessSynchronizer(processSynchronizer);
    //when
    boolean result = processSynchronizer.hasDataToBeIndexed();
    //then
    assertTrue(result);
  }

  private void setupDataQueueForProcessSynchronizer(ProcessSynchronizer processSynchronizer) {
    IntStream.range(0, QUERY_SEGMENT_SIZE).forEach(i -> processSynchronizer.subtractWorkingQueryProcess());
    IntStream.range(0, ProcessConfiguration.getInstance().getUpdateThreadsCount()).forEach(i -> processSynchronizer
        .subtractWorkingUpdatesProcess());
    processSynchronizer.tryFillQueueWithSearchHits(createSearchResponse());
  }

  @Test
  public void shouldHasDataToBeIndexedBeFalseWhenDataQueueIsEmptyAndDataQueueIsEmpty() throws Exception {
    //given
    ProcessSynchronizer processSynchronizer = new ProcessSynchronizer(QUERY_SEGMENT_SIZE);
    IntStream.range(0, QUERY_SEGMENT_SIZE).forEach(i -> processSynchronizer.subtractWorkingQueryProcess());
    //when
    boolean result = processSynchronizer.hasDataToBeIndexed();
    //then
    assertFalse(result);
  }

  @Test
  public void shouldPollWhatHasPulled() throws Exception {
    //given
    ProcessSynchronizer processSynchronizer = new ProcessSynchronizer(QUERY_SEGMENT_SIZE);
    SearchResponse searchResponse = createSearchResponse();
    processSynchronizer.tryFillQueueWithSearchHits(searchResponse);
    //when
    SearchHits searchHits = processSynchronizer.pollDataToIndexed();
    //then
    assertEquals(searchResponse.getHits(), searchHits);
  }

  @Test
  public void shouldReturnEmptyHitsWhenPoolingWithTimeout() throws Exception {
    //given
    ProcessSynchronizer processSynchronizer = new ProcessSynchronizer(QUERY_SEGMENT_SIZE);
    //when
    SearchHits searchHits = processSynchronizer.pollDataToIndexed();
    //then
    assertEquals(0, searchHits.getTotalHits());
  }


  @Test
  public void shouldNotWaitForProcessesToEndWhenWorkingQueueIsEmpty() throws Exception {
    //given
    ProcessSynchronizer processSynchronizer = new ProcessSynchronizer(QUERY_SEGMENT_SIZE);
    IntStream.range(0, QUERY_SEGMENT_SIZE).forEach(i -> processSynchronizer.subtractWorkingQueryProcess());
    IntStream.range(0, ProcessConfiguration.getInstance().getUpdateThreadsCount()).forEach(i -> processSynchronizer.subtractWorkingUpdatesProcess());
    //when
    processSynchronizer.waitForProcessesToEnd();
    //then
    assertFalse(Thread.currentThread().isInterrupted());
  }

  @Test
  public void shouldWaitTillQueriesLatchReleased() throws Exception {
    //given
    ProcessSynchronizer processSynchronizer = new ProcessSynchronizer(QUERY_SEGMENT_SIZE);
    final AtomicBoolean waitedTillSubtractWorkingQueryProcessDone = new AtomicBoolean();
    createTimerReleasingAllProcessesAfterSecond(processSynchronizer, waitedTillSubtractWorkingQueryProcessDone);
    //when
    processSynchronizer.waitForProcessesToEnd();
    //then
    assertEquals(true, waitedTillSubtractWorkingQueryProcessDone.get());
  }

  private void createTimerReleasingAllProcessesAfterSecond(ProcessSynchronizer processSynchronizer, AtomicBoolean
      waitedTillSubtractWorkingQueryProcessDone) {
    Timer timer = new Timer();
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        waitedTillSubtractWorkingQueryProcessDone.set(true);
        IntStream.range(0, QUERY_SEGMENT_SIZE).forEach(i -> processSynchronizer.subtractWorkingQueryProcess());
        IntStream.range(0, ProcessConfiguration.getInstance().getUpdateThreadsCount()).forEach(i -> processSynchronizer.subtractWorkingUpdatesProcess());
      }
    }, 1000);
  }

  private SearchResponse createSearchResponse() {
    SearchResponse searchResponse = mock(SearchResponse.class);
    SearchHits searchHits = mock(SearchHits.class);
    when(searchHits.getHits()).thenReturn(new SearchHit[0]);
    when(searchResponse.getHits()).thenReturn(searchHits);
    return searchResponse;
  }
}

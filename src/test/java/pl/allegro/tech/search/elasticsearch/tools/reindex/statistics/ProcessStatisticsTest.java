package pl.allegro.tech.search.elasticsearch.tools.reindex.statistics;

import org.junit.Test;

import static org.junit.Assert.*;

public class ProcessStatisticsTest {

  @Test
  public void shouldGetEmptyReindexingSummary() throws Exception {
    //given
    ProcessStatistics processStatistics = new ProcessStatistics();
    //when
    ReindexingSummary reindexingSummary = processStatistics.createReindexingSummary();
    //then
    ReindexingSummaryAssert.assertThat(reindexingSummary)
        .hasIndexedCount(0L)
        .hasQueriedCount(0L)
        .hasFailedIndexedCount(0L);
  }

  @Test
  public void shouldCountInIndexing() throws Exception {
    //given
    ProcessStatistics processStatistics = new ProcessStatistics();
    //when
    processStatistics.incrementUpdates(1);
    ReindexingSummary reindexingSummary = processStatistics.createReindexingSummary();
    //then
    assertEquals(1L, reindexingSummary.getIndexed());
  }

  @Test
  public void shouldCountQueries() throws Exception {
    //given
    ProcessStatistics processStatistics = new ProcessStatistics();
    //when
    processStatistics.incrementQueries(10);
    ReindexingSummary reindexingSummary = processStatistics.createReindexingSummary();
    //then
    assertEquals(10L, reindexingSummary.getQueried());
  }

  @Test
  public void shouldCountFailedIndexing() throws Exception {
    //given
    ProcessStatistics processStatistics = new ProcessStatistics();
    //when
    processStatistics.incrementFailures(5);
    ReindexingSummary reindexingSummary = processStatistics.createReindexingSummary();
    //then
    assertEquals(5L, reindexingSummary.getFailedIndexed());
  }

}
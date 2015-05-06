package pl.allegro.tech.search.elasticsearch.tools.reindex.process;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.search.elasticsearch.tools.reindex.statistics.ProcessStatistics;
import pl.allegro.tech.search.elasticsearch.tools.reindex.statistics.ReindexingSummary;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ProcessSynchronizer {

  private static final Logger logger = LoggerFactory.getLogger(ProcessSynchronizer.class);

  private LinkedBlockingQueue<SearchHits> dataQueue;
  private CountDownLatch finishedQueringLatch;
  private CountDownLatch finishedUpdatesLatch;
  private ProcessStatistics statistics = new ProcessStatistics();
  private final int queuePollTimeout = ProcessConfiguration.getInstance().getQueuePollTimeout();
  private final int queueOfferTimeout = ProcessConfiguration.getInstance().getQueueOfferTimeout();
  private final List<Throwable> exceptions = Collections.synchronizedList(new LinkedList<Throwable>());

  public ProcessSynchronizer(int querySegmentCount) {
    this.dataQueue = new LinkedBlockingQueue(ProcessConfiguration.getInstance().getQueueSize());
    this.finishedQueringLatch = new CountDownLatch(querySegmentCount);
    this.finishedUpdatesLatch = new CountDownLatch(ProcessConfiguration.getInstance().getUpdateThreadsCount());
  }

  public void waitForProcessesToEnd() {
    try {
      finishedQueringLatch.await();
      finishedUpdatesLatch.await();
    } catch (InterruptedException e) {
      logger.error("Waiting for processes to end fails", e);
      Thread.currentThread().interrupt();
    }
  }

  public long getWorkingQueryProcessCount() {
    return finishedQueringLatch.getCount();
  }

  public void subtractWorkingQueryProcess() {
    finishedQueringLatch.countDown();
  }

  public void subtractWorkingUpdatesProcess() {
    finishedUpdatesLatch.countDown();
  }

  public void logStats() {
    exceptions.stream().forEach(
        exception -> logger.error("Processing Exception: ", exception)
    );
    statistics.log(dataQueue.size(), getWorkingQueryProcessCount());
  }

  public void incrementUpdates(int indexedCount) {
    statistics.incrementUpdates(indexedCount);
  }

  public void incrementQueries(int delta) {
    statistics.incrementQueries(delta);
  }

  public void incrementFailures(long count) {
    statistics.incrementFailures(count);
  }

  public boolean tryFillQueueWithSearchHits(SearchResponse response) {
    try {
      SearchHits hits = response.getHits();
      dataQueue.offer(hits, queueOfferTimeout, TimeUnit.MINUTES);
      incrementQueries(hits.getHits().length);
      logStats();
      return true;
    } catch (InterruptedException e) {
      logger.error("Fill Query Queue interrupted", e);
      Thread.currentThread().interrupt();
      return false;
    }
  }

  public SearchHits pollDataToIndexed() throws InterruptedException {
    SearchHits polled = dataQueue.poll(queuePollTimeout, TimeUnit.SECONDS);
    if (didTimeout(polled)) {
      return InternalSearchHits.empty();
    }
    return polled;
  }

  private boolean didTimeout(SearchHits polled) {
    return polled == null;
  }

  public boolean hasDataToBeIndexed() {
    return getWorkingQueryProcessCount() > 0 || dataQueue.size() > 0;
  }

  public ReindexingSummary getReindexingSummary() {
    return statistics.createReindexingSummary();
  }

  public void addProcessingException(Exception exception) {
    exceptions.add(exception);
  }
}

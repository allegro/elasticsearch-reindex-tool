package pl.allegro.tech.search.elasticsearch.tools.reindex.statistics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

public class ProcessStatistics {

  private static final Logger logger = LoggerFactory.getLogger(ProcessStatistics.class);

  private final Instant started = Instant.now();
  private final AtomicLong updatesCounter = new AtomicLong();
  private final AtomicLong queriesCounter = new AtomicLong();
  private final AtomicLong failuresCounter = new AtomicLong();


  public void incrementUpdates(int indexedCount) {
    updatesCounter.getAndAdd(indexedCount);
  }

  public void incrementQueries(long delta) {
    queriesCounter.getAndAdd(delta);
  }

  public void log(int queuedCount, long queryProcessCount) {
    logger.info("{} items: {} / {} ({} {}) failed={}",
        Duration.between(started, Instant.now()),
        updatesCounter.get(), queriesCounter.get(), queuedCount, queryProcessCount, failuresCounter.get());
  }

  public void incrementFailures(long delta) {
    failuresCounter.addAndGet(delta);
  }

  public ReindexingSummary createReindexingSummary() {
    return ReindexingSummaryBuilder.builder()
        .setQueried(queriesCounter.get())
        .setIndexed(updatesCounter.get())
        .setFailedIndexed(failuresCounter.get())
        .createReindexingSummary();
  }
}

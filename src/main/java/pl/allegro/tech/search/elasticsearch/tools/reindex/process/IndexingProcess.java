package pl.allegro.tech.search.elasticsearch.tools.reindex.process;

import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticDataPointer;

import java.util.Optional;

public class IndexingProcess implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(IndexingProcessBuilder.class);

  private IndexingComponent indexingComponent;
  private ProcessSynchronizer processSynchronizer;
  private ElasticDataPointer dataPointer;

  public IndexingProcess(IndexingComponent indexingComponent, ProcessSynchronizer processSynchronizer, ElasticDataPointer dataPointer) {
    this.indexingComponent = indexingComponent;
    this.processSynchronizer = processSynchronizer;
    this.dataPointer = dataPointer;
  }

  @Override
  public void run() {
    while (processSynchronizer.hasDataToBeIndexed()) {
      SearchHits hits = null;
      try {
        hits = processSynchronizer.pollDataToIndexed();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("Update Process interrupted", e);
        continue;
      }
      Optional<BulkResult> bulkResult = indexingComponent.indexData(dataPointer, hits.getHits());
      processBulkResult(bulkResult);
    }
    processSynchronizer.subtractWorkingUpdatesProcess();
  }

  private void processBulkResult(Optional<BulkResult> bulkResult) {
    bulkResult.ifPresent(
        bResult -> processSynchronizer.incrementUpdates(bResult.getIndexedCount())
    );
    bulkResult.filter(bResult -> bResult.getFailedCount() > 0).ifPresent(
        bResult -> {
          processSynchronizer.incrementFailures(bResult.getFailedCount());
          logger.warn("Failed indexing documents with ids: {}", bResult.getFailedIds());
        }
    );
  }

}

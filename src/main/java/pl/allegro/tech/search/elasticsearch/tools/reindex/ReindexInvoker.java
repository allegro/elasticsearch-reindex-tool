package pl.allegro.tech.search.elasticsearch.tools.reindex;

import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticDataPointer;
import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticSearchClientFactory;
import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticSearchQuery;
import pl.allegro.tech.search.elasticsearch.tools.reindex.process.IndexingComponent;
import pl.allegro.tech.search.elasticsearch.tools.reindex.process.IndexingProcessBuilder;
import pl.allegro.tech.search.elasticsearch.tools.reindex.process.ProcessConfiguration;
import pl.allegro.tech.search.elasticsearch.tools.reindex.process.ProcessExecutor;
import pl.allegro.tech.search.elasticsearch.tools.reindex.process.ProcessSynchronizer;
import pl.allegro.tech.search.elasticsearch.tools.reindex.process.QueryComponentBuilder;
import pl.allegro.tech.search.elasticsearch.tools.reindex.process.QueryProcess;
import pl.allegro.tech.search.elasticsearch.tools.reindex.query.QuerySegmentation;
import pl.allegro.tech.search.elasticsearch.tools.reindex.statistics.ReindexingSummary;

import java.util.Arrays;
import java.util.stream.IntStream;

class ReindexInvoker {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReindexInvoker.class);

  private ProcessSynchronizer processSynchronizer;
  private ProcessExecutor processExecutor;

  public ReindexInvoker(int querySegmentCount) {
    processExecutor = new ProcessExecutor(querySegmentCount);
    processSynchronizer = new ProcessSynchronizer(querySegmentCount);
  }

  public static ReindexingSummary invokeReindexing(ElasticDataPointer sourcePointer, ElasticDataPointer targetPointer, QuerySegmentation
      segmentation, ElasticSearchQuery query) {
    ReindexInvoker reindexInvoker = new ReindexInvoker(segmentation.getSegmentsCount());
    LOGGER.info("Starting");
    ReindexingSummary summary = reindexInvoker.run(sourcePointer, targetPointer, segmentation, query);
    LOGGER.info("Ended");
    return summary;
  }

  public ReindexingSummary run(ElasticDataPointer sourcePointer, ElasticDataPointer targetPointer, QuerySegmentation segmentation, ElasticSearchQuery query) {
    Client sourceClient = ElasticSearchClientFactory.createClient(sourcePointer);
    Client targetClient = ElasticSearchClientFactory.createClient(targetPointer);

    if (indexExists(sourceClient, sourcePointer.getIndexName())) {
      startQueriesProcesses(sourceClient, sourcePointer, segmentation, query);
      startUpdatesProcesses(targetClient, targetPointer);
      processSynchronizer.waitForProcessesToEnd();
    }

    releaseResources(sourceClient, targetClient);

    processSynchronizer.logStats();
    return processSynchronizer.getReindexingSummary();
  }

  private boolean indexExists(Client sourceClient, String indexName) {
    return sourceClient.admin().indices().prepareExists(indexName).get().isExists();
  }

  private void startUpdatesProcesses(Client client, ElasticDataPointer targetPointer) {
    IntStream.range(0, ProcessConfiguration.getInstance().getUpdateThreadsCount()).forEach(
        i -> processExecutor.startProcess(
            IndexingProcessBuilder.builder()
                .setIndexingComponent(new IndexingComponent(client))
                .setDataPointer(targetPointer)
                .setProcessSynchronizer(processSynchronizer)
                .build())
    );
  }

  private void startQueriesProcesses(Client client, ElasticDataPointer sourcePointer, QuerySegmentation segmentation, ElasticSearchQuery query) {
    IntStream.range(0, segmentation.getSegmentsCount())
        .mapToObj(
            i ->
                QueryComponentBuilder.builder()
                    .setClient(client)
                    .setDataPointer(sourcePointer)
                    .setSegmentationField(segmentation.getFieldName())
                    .setBound(segmentation.getThreshold(i))
                    .setQuery(query)
                    .createQueryComponent()
        ).map(
        queryComponent -> new QueryProcess(processSynchronizer, queryComponent)
    ).forEach(
        processExecutor::startProcess
    );
  }

  private void releaseResources(Client sourceClient, Client targetClient) {
    processExecutor.finishProcessing();
    refreshTargetIndex(targetClient);
    disconnectElasticsearchClients(sourceClient, targetClient);
  }

  private void refreshTargetIndex(Client targetClient) {
    targetClient.admin().indices().prepareRefresh().get();
  }

  private void disconnectElasticsearchClients(Client... clients) {
    Arrays.asList(clients)
        .forEach(Client::close);
  }

}

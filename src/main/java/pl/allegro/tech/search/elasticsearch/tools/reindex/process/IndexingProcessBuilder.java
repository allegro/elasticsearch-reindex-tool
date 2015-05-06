package pl.allegro.tech.search.elasticsearch.tools.reindex.process;

import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticDataPointer;

public final class IndexingProcessBuilder {

  private IndexingComponent indexingComponent;
  private ProcessSynchronizer processSynchronizer;
  private ElasticDataPointer dataPointer;

  private IndexingProcessBuilder() {
  }

  public IndexingProcess build() {
    return new IndexingProcess(indexingComponent, processSynchronizer, dataPointer);
  }

  public IndexingProcessBuilder setProcessSynchronizer(ProcessSynchronizer processSynchronizer) {
    this.processSynchronizer = processSynchronizer;
    return this;
  }

  public IndexingProcessBuilder setDataPointer(ElasticDataPointer dataPointer) {
    this.dataPointer = dataPointer;
    return this;
  }

  public IndexingProcessBuilder setIndexingComponent(IndexingComponent indexingComponent) {
    this.indexingComponent = indexingComponent;
    return this;
  }

  public static IndexingProcessBuilder builder() {
    return new IndexingProcessBuilder();
  }
}

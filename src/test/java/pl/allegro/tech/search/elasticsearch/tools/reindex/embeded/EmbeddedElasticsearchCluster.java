package pl.allegro.tech.search.elasticsearch.tools.reindex.embeded;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.util.List;
import java.util.stream.Stream;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import pl.allegro.tech.search.elasticsearch.tools.reindex.ReindexInvokerTest;
import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticDataPointer;
import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticDataPointerBuilder;
import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticSearchQuery;
import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticSearchQueryBuilder;

public final class EmbeddedElasticsearchCluster {

  public static final String CLUSTER_NAME = "ReindexInvoker_cluster";
  public static final int ELS_PORT = 9211;
  public static final int ELS_TCP_PORT = 9311;

  private final Node dataNode;

  private EmbeddedElasticsearchCluster(String clusterName, int apiPort) {
    NodeBuilder nodeBuilder = nodeBuilder()
            .clusterName(clusterName)
            .data(true);
    ImmutableSettings.Builder settings = nodeBuilder.settings()
            .put("http.port", ELS_PORT)
            .put("index.store.type", "memory")
            .put("transport.tcp.port", apiPort);

    dataNode = nodeBuilder.settings(settings).node();
    dataNode.client().admin().cluster().prepareHealth().setWaitForGreenStatus().get();
  }

  public static EmbeddedElasticsearchCluster createDataNode() {
    return new EmbeddedElasticsearchCluster(CLUSTER_NAME, ELS_TCP_PORT);
  }

  public void recreateIndex(final String sourceIndex) {
    IndicesAdminClient indices = dataNode.client().admin().indices();
    if (indices.prepareExists(sourceIndex).get().isExists()) {
      indices.prepareDelete(sourceIndex).get();
    }
    indices.prepareCreate(sourceIndex).get();
  }

  public void close() {
    dataNode.close();
  }

  public Client client() {
    return dataNode.client();
  }

  public void deleteIndex(String targetIndex) {
    IndicesAdminClient indices = dataNode.client().admin().indices();
    if (indices.prepareExists(targetIndex).get().isExists()) {
      indices.prepareDelete(targetIndex).get();
    }
  }

  public void indexDocument(String index, String type, IndexDocument indexDocument) {
    IndexRequestBuilder requestBuilder = dataNode.client().prepareIndex(index, type, indexDocument.getId()).setSource(indexDocument.getDoc());
    if (indexDocument.getTTL() != null) {
      requestBuilder.setTTL(indexDocument.getTTL());
    }
    requestBuilder.get();
  }

  public boolean indexExist(String index) {
    return dataNode.client().admin().indices().prepareExists(index).get().isExists();
  }

  public long count(String index) {
    return dataNode.client().prepareCount(index).get().getCount();
  }

  public ElasticDataPointer createDataPointer(String indexName) {
    return ElasticDataPointerBuilder.builder()
            .setAddress("http://127.0.0.1:" + ELS_TCP_PORT + "/" + indexName + "/" + ReindexInvokerTest.DATA_TYPE)
            .setClusterName(CLUSTER_NAME)
            .build();
  }

  public void indexWithSampleData(String sourceIndex, String type, Stream<IndexDocument> indexDocumentStream) {
    recreateIndex(sourceIndex);
    indexDocumentStream.forEach(
            indexDocument -> indexDocument(sourceIndex, type, indexDocument)
    );
    refreshIndex();
  }

  public void refreshIndex() {
    dataNode.client().admin().indices().prepareRefresh().get();
  }

  public void createIndex(String index, String type, XContentBuilder mappingDef) {
    dataNode.client().admin().indices()
            .prepareCreate(index)
            .addMapping(type, mappingDef)
            .get();
  }

  public ElasticSearchQuery createInitialQuery(String query) {
    return ElasticSearchQueryBuilder.builder().setQuery(query).build();
  }

  public ElasticSearchQuery createInitialQuery(String query, String orderByField) {
    return ElasticSearchQueryBuilder.builder().setQuery(query).setSortByField(orderByField).build();
  }

  public ElasticSearchQuery createInitialQuery(String query, List<Integer> shards) {
    return ElasticSearchQueryBuilder.builder().setQuery(query).setShards(shards).build();
  }

  public ElasticSearchQuery createInitialQuery(String query, String orderByField, List<Integer> shards) {
    return ElasticSearchQueryBuilder.builder().setQuery(query).setSortByField(orderByField).setShards(shards).build();
  }
}

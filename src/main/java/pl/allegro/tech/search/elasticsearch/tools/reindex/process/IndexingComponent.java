package pl.allegro.tech.search.elasticsearch.tools.reindex.process;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHit;
import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticDataPointer;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IndexingComponent {

  private final Client client;

  public IndexingComponent(Client client) {
    this.client = client;
  }

  private BulkRequestBuilder createBulkRequestBuilder() {
    return client.prepareBulk();
  }

  public Optional<BulkResult> indexData(ElasticDataPointer targetDataPointer, SearchHit[] hits) {
    BulkRequestBuilder bulkRequest = createBulkRequestBuilder();

    for (SearchHit hit : hits) {
      Map<String, Object> source = hit.getSource();
      IndexRequestBuilder requestBuilder = prepareIndex(targetDataPointer.getIndexName(), targetDataPointer
          .getTypeName(), hit.getId());
      if (hit.getFields().get("_ttl") != null) {
        requestBuilder.setTTL(hit.getFields().get("_ttl").value());
      }
      if (hit.getFields().get("_routing") != null) {
        requestBuilder.setRouting(hit.getFields().get("_routing").value());
      }
      requestBuilder.setSource(source);
      bulkRequest.add(requestBuilder);
    }
    return executeBulk(hits.length, bulkRequest);
  }

  private Optional<BulkResult> executeBulk(int indexedCount, BulkRequestBuilder bulkRequest) {
    if (bulkRequest.numberOfActions() > 0) {
      BulkResponse bulkItemResponses = bulkRequest.execute().actionGet();
      Set<String> failedIds = Stream.of(bulkItemResponses.getItems())
          .filter(BulkItemResponse::isFailed)
          .map(BulkItemResponse::getId)
          .collect(Collectors.toSet());
      return Optional.of(new BulkResult(indexedCount, failedIds));
    }
    return Optional.empty();
  }

  private IndexRequestBuilder prepareIndex(String indexName, String typeName, String id) {
    return client.prepareIndex(indexName, typeName, id);
  }

}

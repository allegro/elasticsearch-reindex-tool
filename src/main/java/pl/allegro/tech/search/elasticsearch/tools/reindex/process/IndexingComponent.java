package pl.allegro.tech.search.elasticsearch.tools.reindex.process;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHit;
import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticDataPointer;

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
          .getTypeName(), hit.getId(), source, hit.getIndex());
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

  private static final Pattern INDEX_NAME_REPLACEMENT_PATTERN = Pattern.compile("\\$\\{([^}]+)\\}");
  private IndexRequestBuilder prepareIndex(String indexName, String typeName, String id, Map<String, Object> sourceFields, String sourceIndex) {
    String newIndexName = computeIndexName(indexName, sourceFields, sourceIndex);

    return client.prepareIndex(newIndexName, typeName, id);
  }

  protected static String computeIndexName(String indexName, Map<String, Object> sourceFields, String sourceIndex) {
    StringBuffer sb = new StringBuffer();
    Matcher matcher = INDEX_NAME_REPLACEMENT_PATTERN.matcher(indexName);
    while(matcher.find()) {
      String fieldName = matcher.group(1);
      String format = null;
      int pos = fieldName.indexOf(':');
      if(pos != -1) {
        format = fieldName.substring(pos + 1);
        fieldName = fieldName.substring(0, pos);
      }

      final String replacement;
      if(fieldName.equals("_index")) {
        replacement = sourceIndex;
      } else {
        Object obj = sourceFields.get(fieldName);
        Preconditions.checkNotNull(obj, "Specified source field " + fieldName + " not found for index-name replacement");
        String field = obj.toString();
        if(format != null) {
          // only support time based on milliseconds since the epoch for now
          SimpleDateFormat formatter = new SimpleDateFormat(format);
          replacement = formatter.format(new Date(Long.parseLong(field)));
        } else {
          replacement = field;
        }
      }

      matcher.appendReplacement(sb, replacement);
    }
    matcher.appendTail(sb);
    return sb.toString();
  }

}

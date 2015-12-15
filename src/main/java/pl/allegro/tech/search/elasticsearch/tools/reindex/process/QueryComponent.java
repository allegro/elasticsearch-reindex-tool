package pl.allegro.tech.search.elasticsearch.tools.reindex.process;

import com.google.common.base.Strings;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.sort.FieldSortBuilder;
import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticDataPointer;
import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticSearchQuery;
import pl.allegro.tech.search.elasticsearch.tools.reindex.query.BoundedSegment;
import pl.allegro.tech.search.elasticsearch.tools.reindex.query.filter.BoundedFilterFactory;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class QueryComponent {

  public static final int SCROLL_TIME_LIMIT = 60000;
  public static final int SCROLL_SHARD_LIMIT = 200;
  public static final int SCROLL_TIMEOUT = 600000;

  private Client client;
  private Optional<String> segmentationField;
  private ElasticDataPointer dataPointer;
  private Optional<BoundedSegment> bound;
  private ElasticSearchQuery query;
  private BoundedFilterFactory boundedFilterFactory = new BoundedFilterFactory();

  QueryComponent(Client client, ElasticDataPointer dataPointer, Optional<String> segmentationField, Optional<BoundedSegment> bound, ElasticSearchQuery query) {
    this.client = client;
    this.dataPointer = dataPointer;
    this.segmentationField = segmentationField;
    this.bound = bound;
    this.query = query;
  }

  public SearchResponse prepareSearchScrollRequest() {
    SearchRequestBuilder searchRequestBuilder = client.prepareSearch(dataPointer.getIndexName())
            .setTypes(dataPointer.getTypeName())
            .setSearchType(SearchType.SCAN)
            .addFields("_ttl", "_source")
            .setScroll(new TimeValue(SCROLL_TIME_LIMIT))
            .setSize(SCROLL_SHARD_LIMIT);

    if (!Strings.isNullOrEmpty(query.getQuery())) {
      searchRequestBuilder.setQuery(query.getQuery());
    }
    if (!Strings.isNullOrEmpty(query.getSortField())) {
      searchRequestBuilder.addSort(new FieldSortBuilder(query.getSortField()).order(query.getSortOrder()));
    }
    if (query.getShards() != null && !query.getShards().isEmpty()) {
      String joinedShards = String.join(",", query.getShards().stream().map(shard -> shard.toString()).collect(Collectors.toList()));
      searchRequestBuilder.setPreference("_shards:" + joinedShards);
    }

    bound.map(resolvedBound -> boundedFilterFactory.createBoundedFilter(segmentationField.get(), resolvedBound))
            .ifPresent(searchRequestBuilder::setQuery);

    return searchRequestBuilder.execute().actionGet();
  }

  public SearchResponse getNextScrolledSearchResults(String scrollId) {
    return client.prepareSearchScroll(scrollId)
            .setScroll(new TimeValue(SCROLL_TIMEOUT))
            .get();
  }

  int getResponseSize(SearchResponse response) {
    return response.getHits().getHits().length;
  }

  boolean searchResultsNotEmpty(SearchResponse response) {
    return response.getHits().getTotalHits() > 0;
  }
}

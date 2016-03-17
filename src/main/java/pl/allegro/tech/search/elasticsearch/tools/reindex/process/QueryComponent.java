package pl.allegro.tech.search.elasticsearch.tools.reindex.process;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.base.Strings;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticDataPointer;
import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticSearchQuery;
import pl.allegro.tech.search.elasticsearch.tools.reindex.query.BoundedSegment;
import pl.allegro.tech.search.elasticsearch.tools.reindex.query.filter.BoundedFilterFactory;

import java.util.Optional;

public class QueryComponent {
  private static final Logger logger = LoggerFactory.getLogger(QueryComponent.class);

  public static final int SCROLL_TIME_LIMIT = 60000;
  public static final int SCROLL_SHARD_LIMIT = 5000;
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
    // find out how many indices and shards are affected by this query to not get huge result sets when there are very many indices affected by the name, e.g. when wildcards are used
    // otherwise we regularly run into OOMs when a query goes against a large number of indices
    // I did not find a better way to find out the number of shards than to query a list of indices and for each index query the number of shards via the settings
    GetSettingsResponse getSettingsResponse = client.admin().indices().getSettings(new GetSettingsRequest().indices(dataPointer.getIndexName())).actionGet();
    int numShards = 0, numIndices = 0;
    for(ObjectCursor<Settings> settings : getSettingsResponse.getIndexToSettings().values()) {
      numShards += settings.value.getAsInt("index.number_of_shards", 0);
      numIndices++;
    }

    int sizePerShard = (int)Math.ceil((double)SCROLL_SHARD_LIMIT/numShards);
    logger.info("Found " + numIndices + " indices and " + numShards + " shards matching the index-pattern, thus setting the sizePerShard to " + sizePerShard);

    SearchRequestBuilder searchRequestBuilder = client.prepareSearch(dataPointer.getIndexName())
        .setTypes(dataPointer.getTypeName())
        .setSearchType(SearchType.SCAN)
        .addFields("_ttl", "_source")
        .setScroll(new TimeValue(SCROLL_TIME_LIMIT))
        .setSize(sizePerShard);

    if (!Strings.isNullOrEmpty(query.getQuery())) {
      searchRequestBuilder.setQuery(query.getQuery());
    }
    if (!Strings.isNullOrEmpty(query.getSortField())) {
      searchRequestBuilder.addSort(new FieldSortBuilder(query.getSortField()).order(query.getSortOrder()));
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

package pl.allegro.tech.search.elasticsearch.tools.reindex.process;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticDataPointer;
import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticSearchClientFactory;
import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticSearchQuery;
import pl.allegro.tech.search.elasticsearch.tools.reindex.embeded.EmbeddedElasticsearchCluster;
import pl.allegro.tech.search.elasticsearch.tools.reindex.embeded.IndexDocument;

import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.Assert.*;

public class QueryComponentTest {
  private static final String SOURCE_INDEX = "sourceindex";
  private static final String TARGET_INDEX = "targetindex";
  public static final String DATA_TYPE = "type";

  private static EmbeddedElasticsearchCluster embeddedElasticsearchCluster;

  @BeforeClass
  public static void setUp() throws Exception {
    embeddedElasticsearchCluster = EmbeddedElasticsearchCluster.createDataNode();
  }


  @AfterClass
  public static void tearDown() throws Exception {
    embeddedElasticsearchCluster.close();
  }

  @Before
  public void clearTargetIndex() {
    embeddedElasticsearchCluster.deleteIndex(SOURCE_INDEX);
    embeddedElasticsearchCluster.deleteIndex(TARGET_INDEX);
  }

  @Test
  public void testQueryNoData() {
    // given
    embeddedElasticsearchCluster.recreateIndex(SOURCE_INDEX);
    ElasticDataPointer sourceDataPointer = embeddedElasticsearchCluster.createDataPointer(SOURCE_INDEX);
    Client sourceClient = ElasticSearchClientFactory.createClient(sourceDataPointer);
    ElasticSearchQuery elasticSearchQuery = embeddedElasticsearchCluster.createInitialQuery("");

    // when
    QueryComponent component = QueryComponentBuilder.builder()
            .setClient(sourceClient)
            .setDataPointer(sourceDataPointer)
            .setQuery(elasticSearchQuery)
            .createQueryComponent();
    SearchResponse searchResponse = component.prepareSearchScrollRequest();

    // then
    assertEquals("No results overall",
            0L, searchResponse.getHits().getTotalHits());
    assertEquals("Initially zero documents are loaded",
            0L, searchResponse.getHits().getHits().length);
    assertEquals("Initially zero documents are loaded",
            0L, component.getResponseSize(searchResponse));
    assertFalse("Some documents are found",
            component.searchResultsNotEmpty(searchResponse));

    // when
    searchResponse = component.getNextScrolledSearchResults(searchResponse.getScrollId());

    // then
    assertEquals("No results overall",
            0L, searchResponse.getHits().getTotalHits());
    assertEquals("Initially zero documents are loaded",
            0L, searchResponse.getHits().getHits().length);
    assertEquals("Initially zero documents are loaded",
            0L, component.getResponseSize(searchResponse));
    assertFalse("Some documents are found",
            component.searchResultsNotEmpty(searchResponse));
  }

  @Test
  public void testQueryWithData() {
    // given
    indexWithSampleData(7000);
    ElasticDataPointer sourceDataPointer = embeddedElasticsearchCluster.createDataPointer(SOURCE_INDEX);
    Client sourceClient = ElasticSearchClientFactory.createClient(sourceDataPointer);
    ElasticSearchQuery elasticSearchQuery = embeddedElasticsearchCluster.createInitialQuery("");

    GetSettingsResponse indexSettings = sourceClient.admin().indices().getSettings(new GetSettingsRequest().indices(SOURCE_INDEX)).actionGet();
    assertEquals("We should have an index with 5 shards now",
            "5", indexSettings.getIndexToSettings().get(SOURCE_INDEX).get("index.number_of_shards"));
    assertEquals("We should have an index with one replica now",
            "1", indexSettings.getIndexToSettings().get(SOURCE_INDEX).get("index.number_of_replicas"));

    // when
    QueryComponent component = QueryComponentBuilder.builder()
            .setClient(sourceClient)
            .setDataPointer(sourceDataPointer)
            .setQuery(elasticSearchQuery)
            .createQueryComponent();
    SearchResponse searchResponse = component.prepareSearchScrollRequest();

    // then
    assertEquals("Overall there should be 7000 hits",
            7000L, searchResponse.getHits().getTotalHits());
    assertEquals("Initially zero documents are loaded",
            0L, searchResponse.getHits().getHits().length);
    assertEquals("Initially zero documents are loaded",
            0L, component.getResponseSize(searchResponse));
    assertTrue("Some documents are found",
            component.searchResultsNotEmpty(searchResponse));

    // when
    searchResponse = component.getNextScrolledSearchResults(searchResponse.getScrollId());

    // then
    assertEquals("Overall there should be 7000 hits",
            7000L, searchResponse.getHits().getTotalHits());
    assertEquals("QueryComponent tries to compute the hits to be 5000 on evenly distributed documents, never more!",
            5000L, searchResponse.getHits().getHits().length);
    assertEquals("QueryComponent tries to compute the hits to be 5000 on evenly distributed documents, never more!",
            5000L, component.getResponseSize(searchResponse));
    assertTrue("Some documents are found",
            component.searchResultsNotEmpty(searchResponse));
  }

  // just a simple test to verify that replica-shards are not included in the calculation of results
  // for the "size per shard" setting in scan/scroll-queries
  @Test
  public void testElasticsearchReplicaHandlingInScrolls() {
    // given
    indexWithSampleData(200);
    ElasticDataPointer sourceDataPointer = embeddedElasticsearchCluster.createDataPointer(SOURCE_INDEX);
    Client sourceClient = ElasticSearchClientFactory.createClient(sourceDataPointer);

    GetSettingsResponse indexSettings = sourceClient.admin().indices().getSettings(new GetSettingsRequest().indices(SOURCE_INDEX)).actionGet();
    assertEquals("We should have an index with 5 shards now",
            "5", indexSettings.getIndexToSettings().get(SOURCE_INDEX).get("index.number_of_shards"));
    assertEquals("We should have an index with one replica now",
            "1", indexSettings.getIndexToSettings().get(SOURCE_INDEX).get("index.number_of_replicas"));

    // when
    SearchRequestBuilder searchRequestBuilder = sourceClient.prepareSearch(sourceDataPointer.getIndexName())
            .setTypes(DATA_TYPE)
            .setSearchType(SearchType.SCAN)
            .addFields("_ttl", "_source")
            .setScroll(new TimeValue(QueryComponent.SCROLL_TIME_LIMIT))
            .setSize(10);
    assertNotNull(searchRequestBuilder);

    // then
    SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
    assertEquals("Overall there should be 200 hits",
            200L, searchResponse.getHits().getTotalHits());
    assertEquals("Initially zero documents are loaded",
            0L, searchResponse.getHits().getHits().length);

    // when
    searchResponse = sourceClient.prepareSearchScroll(searchResponse.getScrollId())
            .setScroll(new TimeValue(QueryComponent.SCROLL_TIMEOUT))
            .get();

    // then
    assertEquals(200L, searchResponse.getHits().getTotalHits());
    assertEquals(50L, searchResponse.getHits().getHits().length);
  }


  private void indexWithSampleData(final int numberOfDocuments) {
    Stream<IndexDocument> streamToBeIndexed = IntStream
            .range(1, numberOfDocuments+1)
            .mapToObj(
                    i -> new IndexDocument(Integer.toString(i), ImmutableMap.of("fieldName", i))
            );
    embeddedElasticsearchCluster.indexWithSampleData(SOURCE_INDEX, DATA_TYPE, streamToBeIndexed);
  }
}

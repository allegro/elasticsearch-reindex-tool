package pl.allegro.tech.search.elasticsearch.tools.reindex;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticDataPointer;
import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticSearchQuery;
import pl.allegro.tech.search.elasticsearch.tools.reindex.embeded.EmbeddedElasticsearchCluster;
import pl.allegro.tech.search.elasticsearch.tools.reindex.embeded.IndexDocument;
import pl.allegro.tech.search.elasticsearch.tools.reindex.query.DoubleFieldSegmentation;
import pl.allegro.tech.search.elasticsearch.tools.reindex.query.EmptySegmentation;
import pl.allegro.tech.search.elasticsearch.tools.reindex.query.StringPrefixSegmentation;
import pl.allegro.tech.search.elasticsearch.tools.reindex.statistics.ReindexingSummary;

import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static pl.allegro.tech.search.elasticsearch.tools.reindex.statistics.ReindexingSummaryAssert.assertThat;

public class ReindexInvokerTest {

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
  public void indexingWithoutSegmentingEmpty() throws Exception {
    //given
    embeddedElasticsearchCluster.recreateIndex(SOURCE_INDEX);
    ElasticDataPointer sourceDataPointer = embeddedElasticsearchCluster.createDataPointer(SOURCE_INDEX);
    ElasticDataPointer targetDataPointer = embeddedElasticsearchCluster.createDataPointer(TARGET_INDEX);
    ElasticSearchQuery elasticSearchQuery = embeddedElasticsearchCluster.createInitialQuery("");
    //when
    ReindexInvoker.invokeReindexing(sourceDataPointer, targetDataPointer, EmptySegmentation.createEmptySegmentation(), elasticSearchQuery);
    //then
    assertFalse(embeddedElasticsearchCluster.indexExist(TARGET_INDEX));
  }

  @Test
  public void indexingWithSegmentingEmpty() throws Exception {
    //given
    embeddedElasticsearchCluster.recreateIndex(SOURCE_INDEX);
    ElasticDataPointer sourceDataPointer = embeddedElasticsearchCluster.createDataPointer(SOURCE_INDEX);
    ElasticDataPointer targetDataPointer = embeddedElasticsearchCluster.createDataPointer(TARGET_INDEX);
    ElasticSearchQuery elasticSearchQuery = embeddedElasticsearchCluster.createInitialQuery("");
    //when
    ReindexInvoker.invokeReindexing(sourceDataPointer, targetDataPointer, DoubleFieldSegmentation.create("fieldName",
        Lists.newArrayList(1.0, 3.0)), elasticSearchQuery);
    //then
    assertFalse(embeddedElasticsearchCluster.indexExist(TARGET_INDEX));
  }

  @Test
  public void indexingWithoutSegmenting() throws Exception {
    //given
    indexWithSampleData();
    ElasticDataPointer sourceDataPointer = embeddedElasticsearchCluster.createDataPointer(SOURCE_INDEX);
    ElasticDataPointer targetDataPointer = embeddedElasticsearchCluster.createDataPointer(TARGET_INDEX);
    ElasticSearchQuery elasticSearchQuery = embeddedElasticsearchCluster.createInitialQuery("");
    //when
    ReindexingSummary reindexingSummary = ReindexInvoker.invokeReindexing(sourceDataPointer, targetDataPointer,
        EmptySegmentation.createEmptySegmentation(), elasticSearchQuery);
    //then
    assertEquals(9L, embeddedElasticsearchCluster.count(TARGET_INDEX));
    assertThat(reindexingSummary)
        .hasIndexedCount(9L)
        .hasQueriedCount(9L)
        .hasFailedIndexedCount(0L);
  }


  @Test
  public void indexingWithSegmentingByDoubleField() throws Exception {
    //given
    indexWithSampleData();
    ElasticDataPointer sourceDataPointer = embeddedElasticsearchCluster.createDataPointer(SOURCE_INDEX);
    ElasticDataPointer targetDataPointer = embeddedElasticsearchCluster.createDataPointer(TARGET_INDEX);
    ElasticSearchQuery elasticSearchQuery = embeddedElasticsearchCluster.createInitialQuery("");
    //when
    ReindexingSummary reindexingSummary = ReindexInvoker.invokeReindexing(sourceDataPointer, targetDataPointer, DoubleFieldSegmentation.create("fieldName",
        Lists.newArrayList(1.0, 3.0, 7.0)), elasticSearchQuery);
    //then
    assertEquals(6L, embeddedElasticsearchCluster.count(TARGET_INDEX));
    assertThat(reindexingSummary)
        .hasIndexedCount(6L)
        .hasQueriedCount(6L)
        .hasFailedIndexedCount(0L);
  }

  @Test
  public void indexingWithSegmentingByPrefixOnStringField() throws Exception {
    //given
    indexWithSampleData();
    ElasticDataPointer sourceDataPointer = embeddedElasticsearchCluster.createDataPointer(SOURCE_INDEX);
    ElasticDataPointer targetDataPointer = embeddedElasticsearchCluster.createDataPointer(TARGET_INDEX);
    ElasticSearchQuery elasticSearchQuery = embeddedElasticsearchCluster.createInitialQuery("");
    //when
    ReindexingSummary reindexingSummary = ReindexInvoker.invokeReindexing(sourceDataPointer, targetDataPointer, StringPrefixSegmentation.create("fieldName",
        Lists.newArrayList("1", "2", "3", "4")), elasticSearchQuery);
    //then
    assertEquals(4L, embeddedElasticsearchCluster.count(TARGET_INDEX));
    assertThat(reindexingSummary)
        .hasIndexedCount(4L)
        .hasQueriedCount(4L)
        .hasFailedIndexedCount(0L);
  }


  @Test
  public void indexingWithStartQuery() throws Exception {
    //given
    indexWithSampleData();
    ElasticDataPointer sourceDataPointer = embeddedElasticsearchCluster.createDataPointer(SOURCE_INDEX);
    ElasticDataPointer targetDataPointer = embeddedElasticsearchCluster.createDataPointer(TARGET_INDEX);
    ElasticSearchQuery elasticSearchQuery = embeddedElasticsearchCluster.createInitialQuery("" +
        "{\"range\": {\"fieldName\" : { \"gte\" : \"5\"}}}", new FieldSortBuilder("fieldName"));
    //when
    ReindexingSummary reindexingSummary = ReindexInvoker.invokeReindexing(sourceDataPointer, targetDataPointer,
        EmptySegmentation.createEmptySegmentation(), elasticSearchQuery);
    //then
    assertEquals(5L, embeddedElasticsearchCluster.count(TARGET_INDEX));
    assertThat(reindexingSummary)
        .hasIndexedCount(5L)
        .hasQueriedCount(5L)
        .hasFailedIndexedCount(0L);
  }

  private void indexWithSampleData() {
    Stream<IndexDocument> streamToBeIndexed = IntStream
        .range(1, 10)
        .mapToObj(
            i -> new IndexDocument(Integer.toString(i), ImmutableMap.of("fieldName", i))
        );
    embeddedElasticsearchCluster.indexWithSampleData(SOURCE_INDEX, DATA_TYPE, streamToBeIndexed);
  }

  @Test
  public void tryingReindexNotExistingIndex() throws Exception {
    //given
    embeddedElasticsearchCluster.deleteIndex(SOURCE_INDEX);
    ElasticDataPointer sourceDataPointer = embeddedElasticsearchCluster.createDataPointer(SOURCE_INDEX);
    ElasticDataPointer targetDataPointer = embeddedElasticsearchCluster.createDataPointer(TARGET_INDEX);
    ElasticSearchQuery elasticSearchQuery = embeddedElasticsearchCluster.createInitialQuery("");
    //when
    ReindexInvoker.invokeReindexing(sourceDataPointer, targetDataPointer, EmptySegmentation.createEmptySegmentation(), elasticSearchQuery);
    //then
    assertFalse(embeddedElasticsearchCluster.indexExist(TARGET_INDEX));

  }
}

package pl.allegro.tech.search.elasticsearch.tools.reindex;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHitField;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticDataPointer;
import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticSearchQuery;
import pl.allegro.tech.search.elasticsearch.tools.reindex.embeded.EmbeddedElasticsearchCluster;
import pl.allegro.tech.search.elasticsearch.tools.reindex.embeded.IndexDocument;
import pl.allegro.tech.search.elasticsearch.tools.reindex.query.EmptySegmentation;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.*;

public class TTLTest {

  private static final String SOURCE_INDEX = "sourceindex";
  private static final String TARGET_INDEX = "targetindex";
  private static final String DATA_TYPE = "type";


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
  public void shouldReindexTTL() throws ExecutionException, InterruptedException {
    //given
    embeddedElasticsearchCluster.createIndex(SOURCE_INDEX, DATA_TYPE, mappingWithTTL());
    embeddedElasticsearchCluster.createIndex(TARGET_INDEX, DATA_TYPE, mappingWithTTL());
    ElasticSearchQuery elasticSearchQuery = embeddedElasticsearchCluster.createInitialQuery("");
    indexSampleDataWithTTL();
    ElasticDataPointer sourceDataPointer = embeddedElasticsearchCluster.createDataPointer(SOURCE_INDEX);
    ElasticDataPointer targetDataPointer = embeddedElasticsearchCluster.createDataPointer(TARGET_INDEX);

    //when
    ReindexInvoker.invokeReindexing(sourceDataPointer, targetDataPointer, EmptySegmentation.createEmptySegmentation(), elasticSearchQuery);
    SearchResponse targetResponse = embeddedElasticsearchCluster.client().prepareSearch(TARGET_INDEX).addFields("_ttl").get();

    //then
    assertThat(embeddedElasticsearchCluster.count(SOURCE_INDEX)).isEqualTo(1L);
    assertThat(embeddedElasticsearchCluster.count(TARGET_INDEX)).isEqualTo(1L);

    Map<String, SearchHitField> resultFields = targetResponse.getHits().getAt(0).getFields();
    assertThat(resultFields.containsKey("_ttl"));
    assertThat((Long) resultFields.get("_ttl").value() > 0L);
  }

  private void indexSampleDataWithTTL() {
    Stream<IndexDocument> streamToBeIndexed = IntStream
        .range(1, 2)
        .mapToObj(
            i -> {
              Long ttl = 60000L;
              return new IndexDocument(Integer.toString(i), ImmutableMap.of("fieldName", i), ttl);
            }
        );

    streamToBeIndexed.forEach(
        indexDocument -> embeddedElasticsearchCluster.indexDocument(SOURCE_INDEX, DATA_TYPE, indexDocument)
    );
    embeddedElasticsearchCluster.refreshIndex();
  }

  private XContentBuilder mappingWithTTL() {
    try {
      // @formatter:off
      //How to enable it in intellij see it here: http://stackoverflow.com/questions/3375307/how-to-disable-code-formatting-for-some-part-of-the-code-using-comments
      return XContentFactory.jsonBuilder()
          .startObject()
            .startObject("_ttl").field("enabled", true).endObject()
          .endObject();
      // @formatter:off
    } catch (IOException e) {
      throw new RuntimeException("Failed building index mappingDef", e);
    }
  }


}

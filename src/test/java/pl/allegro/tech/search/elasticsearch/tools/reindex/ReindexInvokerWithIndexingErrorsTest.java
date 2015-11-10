package pl.allegro.tech.search.elasticsearch.tools.reindex;

import com.google.common.collect.ImmutableMap;
import org.assertj.core.api.Assertions;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticDataPointer;
import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticSearchQuery;
import pl.allegro.tech.search.elasticsearch.tools.reindex.embeded.EmbeddedElasticsearchCluster;
import pl.allegro.tech.search.elasticsearch.tools.reindex.embeded.IndexDocument;
import pl.allegro.tech.search.elasticsearch.tools.reindex.query.EmptySegmentation;
import pl.allegro.tech.search.elasticsearch.tools.reindex.statistics.ReindexingSummary;
import pl.allegro.tech.search.elasticsearch.tools.reindex.statistics.ReindexingSummaryAssert;

import java.io.IOException;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ReindexInvokerWithIndexingErrorsTest {

  private static final String SOURCE_INDEX = "sourceindex";
  private static final String TARGET_INDEX = "targetindex";
  private static final String DATA_TYPE = "type";

  private EmbeddedElasticsearchCluster embeddedElasticsearchCluster;

  @Before
  public void setUp() throws Exception {
    embeddedElasticsearchCluster = EmbeddedElasticsearchCluster.createDataNode();
    embeddedElasticsearchCluster.deleteIndex(SOURCE_INDEX);
    embeddedElasticsearchCluster.deleteIndex(TARGET_INDEX);
  }

  @After
  public void tearDown() throws Exception {
    embeddedElasticsearchCluster.close();
  }

  @Test
  public void shouldWarnWhenIndexingFails() throws Exception {
    //given
    indexWithSampleData();
    embeddedElasticsearchCluster.createIndex(TARGET_INDEX, DATA_TYPE, createStrictMappingDefinition());
    ElasticDataPointer sourceDataPointer = embeddedElasticsearchCluster.createDataPointer(SOURCE_INDEX);
    ElasticDataPointer targetDataPointer = embeddedElasticsearchCluster.createDataPointer(TARGET_INDEX);
    ElasticSearchQuery elasticSearchQuery = embeddedElasticsearchCluster.createInitialQuery("");
    //when
    ReindexingSummary reindexingSummary = ReindexInvoker.invokeReindexing(sourceDataPointer, targetDataPointer, EmptySegmentation.createEmptySegmentation(), elasticSearchQuery);
    //then
    Assertions.assertThat(embeddedElasticsearchCluster.count(SOURCE_INDEX)).isEqualTo(8L);
    Assertions.assertThat(embeddedElasticsearchCluster.count(TARGET_INDEX)).isEqualTo(4L);
    ReindexingSummaryAssert.assertThat(reindexingSummary)
        .hasIndexedCount(8L)
        .hasQueriedCount(8L)
        .hasFailedIndexedCount(4L);
  }

  private void indexWithSampleData() {
    Stream<IndexDocument> docsWithField1 =
        createDocsStream(5, i -> new IndexDocument(Integer.toString(i), ImmutableMap.of("field1", i)));
    Stream<IndexDocument> docsWithField2 =
        createDocsStream(5, i -> new IndexDocument(Integer.toString(i + 5), ImmutableMap.of("field2", i)));
    embeddedElasticsearchCluster.indexWithSampleData(SOURCE_INDEX, DATA_TYPE,
        Stream.concat(docsWithField1, docsWithField2));
  }

  @SuppressWarnings("unchecked")
  private Stream<IndexDocument> createDocsStream(int amount, IntFunction docMapper) {
    return IntStream.range(1, amount)
        .mapToObj(docMapper);
  }

  public XContentBuilder createStrictMappingDefinition() {
    try {
      // @formatter:off
      //How to enable it in intellij see it here: http://stackoverflow.com/questions/3375307/how-to-disable-code-formatting-for-some-part-of-the-code-using-comments
      return XContentFactory.jsonBuilder()
          .startObject()
            .field("dynamic", "strict")
            .startObject("properties")
              .startObject("field1")
                .field("type", "string")
              .endObject()
            .endObject()
          .endObject();
      // @formatter:off
    } catch (IOException e) {
      throw new RuntimeException("Failed building index mappingDef", e);
    }
  }
}

package pl.allegro.tech.search.elasticsearch.tools.reindex.query;

import org.junit.Test;
import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticSearchQuery;
import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticSearchQueryBuilder;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class StringPrefixSegmentationTest {

  @Test
  public void checkSegmentationQuery() throws Exception {
    //given
    ElasticSearchQuery query = ElasticSearchQueryBuilder.builder().build();
    //when
    StringPrefixSegmentation segmentation = StringPrefixSegmentation.create("fieldName", Collections.emptyList(), query, false);

    //then
    assertEquals(query, segmentation.getQuery());
  }
}
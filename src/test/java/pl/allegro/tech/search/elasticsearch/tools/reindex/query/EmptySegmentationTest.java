package pl.allegro.tech.search.elasticsearch.tools.reindex.query;

import com.google.common.collect.Lists;
import org.junit.Test;
import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticSearchQuery;
import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticSearchQueryBuilder;

import static org.junit.Assert.*;

public class EmptySegmentationTest {

  @Test
  public void checkSegmentationQuery() throws Exception {
    //given
    ElasticSearchQuery query = ElasticSearchQueryBuilder.builder().build();

    //when
    EmptySegmentation segmentation = EmptySegmentation.createEmptySegmentation(query);

    //then
    assertEquals(query, segmentation.getQuery());
  }
}
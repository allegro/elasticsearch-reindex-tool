package pl.allegro.tech.search.elasticsearch.tools.reindex.query;

import com.google.common.collect.Lists;
import org.junit.Test;
import org.mockito.Mockito;
import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticSearchQuery;
import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticSearchQueryBuilder;

import static org.junit.Assert.assertEquals;
import static pl.allegro.tech.search.elasticsearch.tools.reindex.query.RangeSegmentAssert.assertThat;

public class DoubleFieldSegmentationTest {

  @Test
  public void checkSegmentationFieldName() throws Exception {
    //when
    DoubleFieldSegmentation segmentation = DoubleFieldSegmentation.create("name", Lists.newArrayList(0.0, 1.0), null, false);

    //then
    assertEquals("name", segmentation.getFieldName().get());
  }

  @Test
  public void checkSegmentationQuery() throws Exception {
    //given
    ElasticSearchQuery query = ElasticSearchQueryBuilder.builder().build();

    //when
    DoubleFieldSegmentation segmentation = DoubleFieldSegmentation.create("name", Lists.newArrayList(0.0, 1.0), query, false);

    //then
    assertEquals(query, segmentation.getQuery());
  }

  @Test
  public void checkSegmentationOneThreshold() throws Exception {
    //when
    DoubleFieldSegmentation segmentation = DoubleFieldSegmentation.create("name", Lists.newArrayList(0.0, 1.0), null, false);

    //then
    assertEquals(1, segmentation.getSegmentsCount());
    assertThat((RangeSegment) segmentation.getThreshold(0).get())
            .hasLowerOpenBound(0.0)
            .hasUpperBound(1.0);
  }

  @Test
  public void checkSegmentationDoubleThreshold() throws Exception {
    //when
    DoubleFieldSegmentation segmentation = DoubleFieldSegmentation.create("name", Lists.newArrayList(0.0, 1.0, 2.0), null, false);

    //then
    assertEquals(2, segmentation.getSegmentsCount());
    assertThat((RangeSegment) segmentation.getThreshold(0).get())
            .hasLowerOpenBound(0.0)
            .hasUpperBound(1.0);
    assertThat((RangeSegment) segmentation.getThreshold(1).get())
            .hasLowerOpenBound(1.0)
            .hasUpperBound(2.0);
  }


}
package pl.allegro.tech.search.elasticsearch.tools.reindex.query;

import com.google.common.collect.Lists;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;
import pl.allegro.tech.search.elasticsearch.tools.reindex.command.ReindexCommand;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static pl.allegro.tech.search.elasticsearch.tools.reindex.query.PrefixSegmentAssert.assertThat;
import static pl.allegro.tech.search.elasticsearch.tools.reindex.query.QuerySegmentationAssert.assertThat;
import static pl.allegro.tech.search.elasticsearch.tools.reindex.query.RangeSegmentAssert.*;

@RunWith(JUnitParamsRunner.class)
public class QuerySegmentationFactoryTest {

  @Test
  public void shouldCreateEmptySegmentationWhenNoSegmentationFieldGiven() throws Exception {
    //given
    ReindexCommand command = mock(ReindexCommand.class);
    when(command.getSegmentationField()).thenReturn(null);
    //when
    QuerySegmentation querySegmentation = QuerySegmentationFactory.create(command);
    //then
    assertEquals(EmptySegmentation.class, querySegmentation.getClass());
  }

  @Test
  @Parameters({ "1.0, 2.0" })
  public void shouldCreateDoubleFieldSegmentation(double lowerBound, double upperBound) throws Exception {
    //given
    ReindexCommand command = mock(ReindexCommand.class);
    String fieldName = "fieldName";
    when(command.getSegmentationField()).thenReturn(fieldName);
    when(command.getSegmentationThresholds()).thenReturn(Lists.newArrayList(lowerBound, upperBound));
    //when
    QuerySegmentation querySegmentation = QuerySegmentationFactory.create(command);
    //then
    assertThat(querySegmentation)
        .isInstanceOf(DoubleFieldSegmentation.class)
        .hasFileName(fieldName)
        .hasSegmentsCount(1);
    RangeSegmentAssert.assertThat((RangeSegment) (querySegmentation.getThreshold(0).get()))
        .hasLowerOpenBound(lowerBound)
        .hasUpperBound(upperBound);
  }

  @Test
  @Parameters({ "1, 2" })
  public void shouldCreateStringPrefixFieldSegmentation(String firstPrefix, String secondPrefix) throws Exception {
    //given
    ReindexCommand command = mock(ReindexCommand.class);
    String fieldName = "fieldName";
    when(command.getSegmentationField()).thenReturn(fieldName);
    when(command.getSegmentationPrefixes()).thenReturn(Lists.newArrayList(firstPrefix, secondPrefix));
    //when
    QuerySegmentation querySegmentation = QuerySegmentationFactory.create(command);
    //then
    assertThat(querySegmentation)
        .isInstanceOf(StringPrefixSegmentation.class)
        .hasFileName(fieldName)
        .hasSegmentsCount(2);
    assertThat((PrefixSegment) querySegmentation.getThreshold(0).get())
        .hasPrefix(firstPrefix);
    assertThat((PrefixSegment) querySegmentation.getThreshold(1).get())
        .hasPrefix(secondPrefix);
  }

  @Test(expected = BadSegmentationDefinitionException.class)
  public void shouldThrowExceptionWhenBadSegmentationDefinition() throws Exception {
    //given
    ReindexCommand command = mock(ReindexCommand.class);
    String fieldName = "fieldName";
    when(command.getSegmentationField()).thenReturn(fieldName);
    //when
    QuerySegmentation querySegmentation = QuerySegmentationFactory.create(command);
    //then
    throw new RuntimeException("shouldn't create segmentation for fieldName " + querySegmentation.getFieldName());
  }


}
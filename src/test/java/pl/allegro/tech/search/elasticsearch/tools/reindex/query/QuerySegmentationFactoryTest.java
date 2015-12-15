package pl.allegro.tech.search.elasticsearch.tools.reindex.query;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;
import pl.allegro.tech.search.elasticsearch.tools.reindex.command.ReindexCommand;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static pl.allegro.tech.search.elasticsearch.tools.reindex.query.PrefixSegmentAssert.assertThat;
import static pl.allegro.tech.search.elasticsearch.tools.reindex.query.QuerySegmentationAssert.assertThat;
import static pl.allegro.tech.search.elasticsearch.tools.reindex.query.RangeSegmentAssert.*;

@SuppressWarnings("Duplicates")
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
  @Parameters({ "1.0, 2.0" })
  public void shouldCreateDoubleFieldSegmentationWithSegmentationByShards(double lowerBound, double upperBound) throws Exception {
    //given
    ReindexCommand command = mock(ReindexCommand.class);
    String fieldName = "fieldName";
    when(command.getSegmentationField()).thenReturn(fieldName);
    when(command.getSegmentationThresholds()).thenReturn(Lists.newArrayList(lowerBound, upperBound));
    when(command.getSegmentationByShards()).thenReturn(true);
    when(command.getShards()).thenReturn(Arrays.asList(0, 1, 2, 3, 4));
    //when
    QuerySegmentation querySegmentation = QuerySegmentationFactory.create(command);
    //then
    assertThat(querySegmentation)
            .isInstanceOf(DoubleFieldSegmentation.class)
            .hasFileName(fieldName)
            .hasSegmentsCount(5);
    for(int i = 0 ; i < 5; i++) {
      RangeSegmentAssert.assertThat((RangeSegment) (querySegmentation.getThreshold(i).get()))
              .hasLowerOpenBound(lowerBound)
              .hasUpperBound(upperBound);
    }
  }

  @Test
  @Parameters({ "1.0, 2.0" })
  public void shouldCreateDoubleFieldSegmentationWithSegmentationByShardsWithSingleShard(double lowerBound, double upperBound) throws Exception {
    //given
    ReindexCommand command = mock(ReindexCommand.class);
    String fieldName = "fieldName";
    when(command.getSegmentationField()).thenReturn(fieldName);
    when(command.getSegmentationThresholds()).thenReturn(Lists.newArrayList(lowerBound, upperBound));
    when(command.getSegmentationByShards()).thenReturn(true);
    when(command.getShards()).thenReturn(Arrays.asList(0));
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
  @Parameters({ "1.0, 2.0" })
  public void shouldCreateDoubleFieldSegmentationWithSingleShard(double lowerBound, double upperBound) throws Exception {
    //given
    ReindexCommand command = mock(ReindexCommand.class);
    String fieldName = "fieldName";
    when(command.getSegmentationField()).thenReturn(fieldName);
    when(command.getSegmentationThresholds()).thenReturn(Lists.newArrayList(lowerBound, upperBound));
    when(command.getSegmentationByShards()).thenReturn(false);
    when(command.getShards()).thenReturn(Arrays.asList(0));
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
  @Parameters({ "1.0, 2.0" })
  public void shouldCreateDoubleFieldSegmentationWithMultipleShards(double lowerBound, double upperBound) throws Exception {
    //given
    ReindexCommand command = mock(ReindexCommand.class);
    String fieldName = "fieldName";
    when(command.getSegmentationField()).thenReturn(fieldName);
    when(command.getSegmentationThresholds()).thenReturn(Lists.newArrayList(lowerBound, upperBound));
    when(command.getSegmentationByShards()).thenReturn(false);
    when(command.getShards()).thenReturn(Arrays.asList(0, 1, 2, 3, 4));
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

  @Test
  @Parameters({ "1, 2" })
  public void shouldCreateStringPrefixFieldSegmentationWithSegmentationByShards(String firstPrefix, String secondPrefix) throws Exception {
    //given
    ReindexCommand command = mock(ReindexCommand.class);
    String fieldName = "fieldName";
    when(command.getSegmentationField()).thenReturn(fieldName);
    when(command.getSegmentationPrefixes()).thenReturn(Lists.newArrayList(firstPrefix, secondPrefix));
    when(command.getSegmentationByShards()).thenReturn(true);
    when(command.getShards()).thenReturn(Arrays.asList(0, 1, 2, 3, 4));
    //when
    QuerySegmentation querySegmentation = QuerySegmentationFactory.create(command);
    //then
    assertThat(querySegmentation)
            .isInstanceOf(StringPrefixSegmentation.class)
            .hasFileName(fieldName)
            .hasSegmentsCount(10);
    for(int i = 0; i < 5; i++) {
      assertThat((PrefixSegment) querySegmentation.getThreshold(i * 2).get())
              .hasPrefix(firstPrefix);
      assertThat((PrefixSegment) querySegmentation.getThreshold(i * 2 + 1).get())
              .hasPrefix(secondPrefix);
    }
  }

  @Test
  @Parameters({ "1, 2" })
  public void shouldCreateStringPrefixFieldSegmentationWithSegmentationByShardsWithSingleShard(String firstPrefix, String secondPrefix) throws Exception {
    //given
    ReindexCommand command = mock(ReindexCommand.class);
    String fieldName = "fieldName";
    when(command.getSegmentationField()).thenReturn(fieldName);
    when(command.getSegmentationPrefixes()).thenReturn(Lists.newArrayList(firstPrefix, secondPrefix));
    when(command.getSegmentationByShards()).thenReturn(true);
    when(command.getShards()).thenReturn(Arrays.asList(0));
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

  @SuppressWarnings("Duplicates")
  @Test
  @Parameters({ "1, 2" })
  public void shouldCreateStringPrefixFieldSegmentationWithSingleShard(String firstPrefix, String secondPrefix) throws Exception {
    //given
    ReindexCommand command = mock(ReindexCommand.class);
    String fieldName = "fieldName";
    when(command.getSegmentationField()).thenReturn(fieldName);
    when(command.getSegmentationPrefixes()).thenReturn(Lists.newArrayList(firstPrefix, secondPrefix));
    when(command.getSegmentationByShards()).thenReturn(false);
    when(command.getShards()).thenReturn(Arrays.asList(0));
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

  @Test
  @Parameters({ "1, 2" })
  public void shouldCreateStringPrefixFieldSegmentationWithMultipleShards(String firstPrefix, String secondPrefix) throws Exception {
    //given
    ReindexCommand command = mock(ReindexCommand.class);
    String fieldName = "fieldName";
    when(command.getSegmentationField()).thenReturn(fieldName);
    when(command.getSegmentationPrefixes()).thenReturn(Lists.newArrayList(firstPrefix, secondPrefix));
    when(command.getSegmentationByShards()).thenReturn(false);
    when(command.getShards()).thenReturn(Arrays.asList(0, 1, 2, 3, 4));
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

  @Test
  public void shouldCreateOnlyShardSegmentationWithMultipleShards() throws Exception {
    //given
    ReindexCommand command = mock(ReindexCommand.class);
    when(command.getSegmentationByShards()).thenReturn(true);
    when(command.getShards()).thenReturn(Arrays.asList(0, 1, 2, 3, 4));
    //when
    QuerySegmentation querySegmentation = QuerySegmentationFactory.create(command);
    //then
    assertThat(querySegmentation)
            .isInstanceOf(OnlyShardSegmentation.class)
            .hasSegmentsCount(5);
  }

  @Test
  public void shouldCreateOnlyShardSegmentationWithSingleShards() throws Exception {
    //given
    ReindexCommand command = mock(ReindexCommand.class);
    when(command.getSegmentationByShards()).thenReturn(true);
    when(command.getShards()).thenReturn(Arrays.asList(0));
    //when
    QuerySegmentation querySegmentation = QuerySegmentationFactory.create(command);
    //then
    assertThat(querySegmentation)
            .isInstanceOf(OnlyShardSegmentation.class)
            .hasSegmentsCount(1);
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

  @Test(expected = BadSegmentationDefinitionException.class)
  public void shouldThrowExceptionWhenBadSegmentationDefinitionWithoutShards() throws Exception {
    //given
    ReindexCommand command = mock(ReindexCommand.class);
    when(command.getSegmentationByShards()).thenReturn(true);
    //when
    QuerySegmentation querySegmentation = QuerySegmentationFactory.create(command);
    //then
    throw new RuntimeException("shouldn't create segmentation without shards when specifying segmentationByShards");
  }
}
package pl.allegro.tech.search.elasticsearch.tools.reindex;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import pl.allegro.tech.search.elasticsearch.tools.reindex.query.PrefixSegment;
import pl.allegro.tech.search.elasticsearch.tools.reindex.query.RangeSegment;

import java.util.Optional;

import static pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticDataPointerAssert.assertThat;
import static pl.allegro.tech.search.elasticsearch.tools.reindex.query.PrefixSegmentAssert.assertThat;
import static pl.allegro.tech.search.elasticsearch.tools.reindex.query.QuerySegmentationAssert.assertThat;
import static pl.allegro.tech.search.elasticsearch.tools.reindex.query.RangeSegmentAssert.assertThat;

@RunWith(JUnitParamsRunner.class)
public class ReindexCommandParserTest {

  @Test
  public void parsesCommandWithNoSegmentation() throws Exception {
    //given
    ReindexCommandParser commandParser = new ReindexCommandParser();
    //when
    boolean result = commandParser.tryParse(createArgumentArray(
        "-sc", "sourceClusterName",
        "-tc", "targetClusterName",
        "-s", "http://sourceHost1:9333/source_index/source_type",
        "-t", "http://targetHost1:9333/target_index/target_type"
    ));
    //then
    Assert.assertEquals(true, result);
    assertThat(commandParser.getSourcePointer())
        .hasHost("sourceHost1")
        .hasClusterName("sourceClusterName")
        .hasPort(9333)
        .hasIndexName("source_index")
        .hasTypeName("source_type");
    assertThat(commandParser.getTargetPointer())
        .hasHost("targetHost1")
        .hasClusterName("targetClusterName")
        .hasPort(9333)
        .hasIndexName("target_index")
        .hasTypeName("target_type");
    Assert.assertEquals(Optional.empty(), commandParser.getSegmentation().getFieldName());
  }

  private Object[] prepareDoubleSegmentationParams() {
    return new Object[] {
        new Object[] { "0,1", 0.0, 1.0 },
        new Object[] { "1.1,230.1", 1.1, 230.1 }
    };
  }

  @Test
  @Parameters(method = "prepareDoubleSegmentationParams")
  public void parsesCommandWithDoubleSegmentation(String segmentationThresholds, double lowerBound, double upperBound) throws
      Exception {
    //given
    ReindexCommandParser commandParser = new ReindexCommandParser();
    //when
    boolean result = commandParser.tryParse(createArgumentArray(
        "-sc", "sourceClusterName",
        "-tc", "targetClusterName",
        "-s", "http://sourceHost1:9333/source_index/source_type",
        "-t", "http://targetHost1:9333/target_index/target_type",
        "-segmentationField", "fieldName",
        "-segmentationThresholds", segmentationThresholds
    ));
    //then
    Assert.assertEquals(true, result);
    assertThat(commandParser.getSegmentation())
        .hasFileName("fieldName")
        .hasSegmentsCount(1);
    assertThat((RangeSegment) commandParser.getSegmentation().getThreshold(0).get())
        .hasUpperBound(upperBound)
        .hasLowerOpenBound(lowerBound);
  }

  @Test
  public void parsesCommandWithStringPrefixSegmentation() throws Exception {
    //given
    ReindexCommandParser commandParser = new ReindexCommandParser();
    //when
    boolean result = commandParser.tryParse(createArgumentArray(
        "-sc", "sourceClusterName",
        "-tc", "targetClusterName",
        "-s", "http://sourceHost1:9333/source_index/source_type",
        "-t", "http://targetHost1:9333/target_index/target_type",
        "-segmentationField", "fieldName",
        "-segmentationPrefixes", "1,2"
    ));
    //then
    Assert.assertEquals(true, result);
    assertThat(commandParser.getSegmentation())
        .hasFileName("fieldName")
        .hasSegmentsCount(2);
    assertThat((PrefixSegment) commandParser.getSegmentation().getThreshold(0).get())
        .hasPrefix("1");
    assertThat((PrefixSegment) commandParser.getSegmentation().getThreshold(1).get())
        .hasPrefix("2");
  }

  @Test
  public void doNotParseCommandWithNoTargetType() throws Exception {
    //given
    ReindexCommandParser commandParser = new ReindexCommandParser();
    //when
    boolean result = commandParser.tryParse(createArgumentArray(
        "-sc", "sourceClusterName",
        "-s", "http://sourceHost1:9333/source_index/source_type",
        "-t", "http://targetHost1:9333/target_index/target_type"
    ));
    //then
    Assert.assertEquals(false, result);

  }

  @Test
  public void parseCommandWithoutSortOrderAndQueryAndSortField() {
    //given
    ReindexCommandParser commandParser = new ReindexCommandParser();
    //when
    commandParser.tryParse(createArgumentArray(
        "-sc", "sourceClusterName",
        "-tc", "targetClusterName",
        "-s", "http://sourceHost1:9333/source_index/source_type",
        "-t", "http://targetHost1:9333/target_index/target_type"
    ));
    //then
    Assert.assertNull(commandParser.getSegmentation().getQuery().getQuery());
  }

  @Test
  public void parseCommandWithQuery() {
    //given
    ReindexCommandParser commandParser = new ReindexCommandParser();
    //when
    String query = "{range\": {\"_timestamp\" : {\"gte\" : 1447142880000}}}";
    commandParser.tryParse(createArgumentArray(
        "-sc", "sourceClusterName",
        "-tc", "targetClusterName",
        "-s", "http://sourceHost1:9333/source_index/source_type",
        "-t", "http://targetHost1:9333/target_index/target_type",
        "-query", query
    ));
    //then
    Assert.assertEquals(query, commandParser.getSegmentation().getQuery().getQuery());
  }

  @Test
  public void parseCommandWithSortField() {
    //given
    ReindexCommandParser commandParser = new ReindexCommandParser();
    //when
    boolean result = commandParser.tryParse(createArgumentArray(
        "-sc", "sourceClusterName",
        "-tc", "targetClusterName",
        "-s", "http://sourceHost1:9333/source_index/source_type",
        "-t", "http://targetHost1:9333/target_index/target_type",
        "-sort", "_timestamp"
    ));
    //then
    Assert.assertEquals("_timestamp", commandParser.getSegmentation().getQuery().getSortField());
  }

  @Test
  public void parseCommandWithSortOrderDESC() {
    //given
    ReindexCommandParser commandParser = new ReindexCommandParser();
    //when
    boolean result = commandParser.tryParse(createArgumentArray(
        "-sc", "sourceClusterName",
        "-tc", "targetClusterName",
        "-s", "http://sourceHost1:9333/source_index/source_type",
        "-t", "http://targetHost1:9333/target_index/target_type",
        "-sortOrder", "DESC"
    ));
    //then
    Assert.assertEquals(SortOrder.DESC, commandParser.getSegmentation().getQuery().getSortOrder());
  }

  @Test
  public void parseCommandWithSortOrderASC() {
    //given
    ReindexCommandParser commandParser = new ReindexCommandParser();
    //when
    boolean result = commandParser.tryParse(createArgumentArray(
        "-sc", "sourceClusterName",
        "-tc", "targetClusterName",
        "-s", "http://sourceHost1:9333/source_index/source_type",
        "-t", "http://targetHost1:9333/target_index/target_type",
        "-sortOrder", "ASC"
    ));
    //then
    Assert.assertEquals(SortOrder.ASC, commandParser.getSegmentation().getQuery().getSortOrder());
  }

  @Test
  public void parseCommandWithSortOrderDefaultASC() {
    //given
    ReindexCommandParser commandParser = new ReindexCommandParser();
    //when
    boolean result = commandParser.tryParse(createArgumentArray(
        "-sc", "sourceClusterName",
        "-tc", "targetClusterName",
        "-s", "http://sourceHost1:9333/source_index/source_type",
        "-t", "http://targetHost1:9333/target_index/target_type"
    ));
    //then
    Assert.assertEquals(SortOrder.ASC, commandParser.getSegmentation().getQuery().getSortOrder());
  }

  @Test
  public void parseDisableClusterSniffing() {
    //given
    ReindexCommandParser commandParser = new ReindexCommandParser();
    //when
    boolean result = commandParser.tryParse(createArgumentArray(
        "-sc", "sourceClusterName",
        "-tc", "targetClusterName",
        "-s", "http://sourceHost1:9333/source_index/source_type",
        "-t", "http://targetHost1:9333/target_index/target_type",
        "-disable-cluster-sniffing"
    ));
    //then
    Assert.assertFalse(commandParser.getSourcePointer().isSniff());
    Assert.assertFalse(commandParser.getTargetPointer().isSniff());
  }

  @Test
  public void parseWhenNotDisabledClusterSniffing() {
    //given
    ReindexCommandParser commandParser = new ReindexCommandParser();
    //when
    boolean result = commandParser.tryParse(createArgumentArray(
        "-sc", "sourceClusterName",
        "-tc", "targetClusterName",
        "-s", "http://sourceHost1:9333/source_index/source_type",
        "-t", "http://targetHost1:9333/target_index/target_type"
    ));
    //then
    Assert.assertTrue(commandParser.getSourcePointer().isSniff());
    Assert.assertTrue(commandParser.getTargetPointer().isSniff());

  }

  private String[] createArgumentArray(String... args) {
    return args;
  }
}

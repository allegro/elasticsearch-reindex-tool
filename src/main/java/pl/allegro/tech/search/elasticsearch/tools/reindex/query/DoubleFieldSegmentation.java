package pl.allegro.tech.search.elasticsearch.tools.reindex.query;

import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticSearchQuery;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public final class DoubleFieldSegmentation extends SegmentationQueryTrait implements QuerySegmentation {

  private final List<Double> thresholds;

  private final Optional<String> fieldName;

  private DoubleFieldSegmentation(String fieldName, List<Double> thresholds, ElasticSearchQuery query, Boolean segmentationByShards) {
    super(query, segmentationByShards);
    this.fieldName = Optional.of(fieldName);
    this.thresholds = Collections.unmodifiableList(thresholds);
  }

  @Override
  public Optional<String> getFieldName() {
    return fieldName;
  }

  @Override
  public int getSegmentsCount() {
    if (this.getSegmentationByShards() && this.getQuery().getShards().size() > 1) {
      return (thresholds.size() - 1) * this.getQuery().getShards().size();
    }

    return thresholds.size() - 1;
  }

  @Override
  public Optional<BoundedSegment> getThreshold(int i) {
    int thresholdIndex = i;
    if (getSegmentationByShards() && this.getQuery().getShards().size() > 1) {
      thresholdIndex = i % (thresholds.size() - 1);
    }

    RangeSegment segmentation =
            RangeSegmentBuilder.builder()
                    .setLowerOpenBound(thresholds.get(thresholdIndex))
                    .setUpperBound(thresholds.get(thresholdIndex + 1))
                    .createRangeSegment();

    return Optional.of(segmentation);
  }

  @Override
  public ElasticSearchQuery getQuery(int i) {
    if (getSegmentationByShards() && this.getQuery().getShards().size() > 1) {
      return getQuery().withShards(Arrays.asList(getQuery().getShards().get(i / (thresholds.size() - 1))));
    }

    return getQuery();
  }

  public static DoubleFieldSegmentation create(String fieldName, List<Double> thresholds, ElasticSearchQuery query, Boolean segmentationByShards) {
    return new DoubleFieldSegmentation(fieldName, thresholds, query, segmentationByShards);
  }
}

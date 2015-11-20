package pl.allegro.tech.search.elasticsearch.tools.reindex.query;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public final class DoubleFieldSegmentation implements QuerySegmentation {

  private final List<Double> thresholds;

  private final Optional<String> fieldName;

  private DoubleFieldSegmentation(String fieldName, List<Double> thresholds) {
    this.fieldName = Optional.of(fieldName);
    this.thresholds = Collections.unmodifiableList(thresholds);
  }

  @Override
  public Optional<String> getFieldName() {
    return fieldName;
  }

  @Override
  public int getSegmentsCount() {
    return thresholds.size() - 1;
  }

  @Override
  public Optional<BoundedSegment> getThreshold(int i) {
    RangeSegment segmentation =
        RangeSegmentBuilder.builder()
            .setLowerOpenBound(thresholds.get(i))
            .setUpperBound(thresholds.get(i + 1))
            .createRangeSegment();
    return Optional.of(segmentation);
  }

  public static DoubleFieldSegmentation create(String fieldName, List<Double> thresholds) {
    return new DoubleFieldSegmentation(fieldName, thresholds);
  }
}

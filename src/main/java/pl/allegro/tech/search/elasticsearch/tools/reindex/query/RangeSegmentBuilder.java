package pl.allegro.tech.search.elasticsearch.tools.reindex.query;

public class RangeSegmentBuilder {
  private Double upperBound;
  private Double lowerOpenBound;

  public RangeSegmentBuilder setUpperBound(Double upperBound) {
    this.upperBound = upperBound;
    return this;
  }

  public RangeSegmentBuilder setLowerOpenBound(Double lowerOpenBound) {
    this.lowerOpenBound = lowerOpenBound;
    return this;
  }

  public RangeSegment createRangeSegment() {
    return new RangeSegment(upperBound, lowerOpenBound);
  }

  public static RangeSegmentBuilder builder() {
    return new RangeSegmentBuilder();
  }
}
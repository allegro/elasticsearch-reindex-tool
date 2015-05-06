package pl.allegro.tech.search.elasticsearch.tools.reindex.query;

public class RangeSegment implements BoundedSegment {

  private final Double upperBound;
  private final Double lowerOpenBound;

  public RangeSegment(Double upperBound, Double lowerOpenBound) {
    this.upperBound = upperBound;
    this.lowerOpenBound = lowerOpenBound;
  }

  public Double getLowerOpenBound() {
    return lowerOpenBound;
  }

  public Double getUpperBound() {
    return upperBound;
  }
}

package pl.allegro.tech.search.elasticsearch.tools.reindex.query;

import org.assertj.core.api.AbstractAssert;

public class RangeSegmentAssert extends AbstractAssert<RangeSegmentAssert, RangeSegment> {

  protected RangeSegmentAssert(RangeSegment actual) {
    super(actual, RangeSegmentAssert.class);
  }

  public static RangeSegmentAssert assertThat(RangeSegment actual) {
    return new RangeSegmentAssert(actual);
  }

  public RangeSegmentAssert hasLowerOpenBound(Double lowerOpenBound) {
    isNotNull();
    if (!actual.getLowerOpenBound().equals(lowerOpenBound)) {
      failWithMessage("Expected lowerOpenBound to be <%f> but was <%f>", lowerOpenBound, actual.getLowerOpenBound());
    }
    return this;
  }

  public RangeSegmentAssert hasUpperBound(Double upperBound) {
    isNotNull();
    if (!actual.getUpperBound().equals(upperBound)) {
      failWithMessage("Expected upperBound to be <%f> but was <%f>", upperBound, actual.getUpperBound());
    }
    return this;
  }

}
package pl.allegro.tech.search.elasticsearch.tools.reindex.query;

import junit.framework.TestCase;
import org.assertj.core.api.AbstractAssert;

public class PrefixSegmentAssert  extends AbstractAssert<PrefixSegmentAssert, PrefixSegment> {

  protected PrefixSegmentAssert(PrefixSegment actual) {
    super(actual, PrefixSegmentAssert.class);
  }

  public static PrefixSegmentAssert assertThat(PrefixSegment actual) {
    return new PrefixSegmentAssert(actual);
  }

  public PrefixSegmentAssert hasPrefix(String prefix) {
    isNotNull();
    if (!actual.getPrefix().equals(prefix)) {
      failWithMessage("Expected character's prefix to be <%s> but was <%s>", prefix, actual.getPrefix());
    }
    return this;
  }

}
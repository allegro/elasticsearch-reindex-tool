package pl.allegro.tech.search.elasticsearch.tools.reindex.query;

import junit.framework.TestCase;
import org.assertj.core.api.AbstractAssert;

public class QuerySegmentationAssert extends AbstractAssert<QuerySegmentationAssert, QuerySegmentation> {

  protected QuerySegmentationAssert(QuerySegmentation actual) {
    super(actual, QuerySegmentationAssert.class);
  }

  public static QuerySegmentationAssert assertThat(QuerySegmentation actual) {
    return new QuerySegmentationAssert(actual);
  }

  public QuerySegmentationAssert isInstanceOf(Class clazz) {
    isNotNull();
    if (!actual.getClass().isAssignableFrom(clazz)) {
      failWithMessage("Expected instance class to be <%s> but was <%s>", clazz, actual.getClass());
    }
    return this;
  }

  public QuerySegmentationAssert hasFileName(String fileName) {
    isNotNull();
    if (!actual.getFieldName().get().equals(fileName)) {
      failWithMessage("Expected character's fileName to be <%s> but was <%s>", fileName, actual.getFieldName().get());
    }
    return this;
  }

  public QuerySegmentationAssert hasSegmentsCount(int segmentsCount) {
    isNotNull();
    if (actual.getSegmentsCount() != segmentsCount) {
      failWithMessage("Expected segmentsCount to be <%d> but was <%d>", segmentsCount, actual.getSegmentsCount());
    }
    return this;
  }

}
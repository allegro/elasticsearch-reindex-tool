package pl.allegro.tech.search.elasticsearch.tools.reindex.connection;

import junit.framework.TestCase;
import org.assertj.core.api.AbstractAssert;

public class ElasticDataPointerAssert extends AbstractAssert<ElasticDataPointerAssert, ElasticDataPointer> {

  public ElasticDataPointerAssert(ElasticDataPointer actual) {
    super(actual, ElasticDataPointerAssert.class);
  }

  public static ElasticDataPointerAssert assertThat(ElasticDataPointer actual) {
    return new ElasticDataPointerAssert(actual);
  }

  public ElasticDataPointerAssert hasHost(String host) {
    isNotNull();
    if (!actual.getHost().equals(host)) {
      failWithMessage("Expected character's host to be <%s> but was <%s>", host, actual.getHost());
    }
    return this;
  }

  public ElasticDataPointerAssert hasPort(int port) {
    isNotNull();
    if (actual.getPort() != port) {
      failWithMessage("Expected port to be <%d> but was <%d>", port, actual.getPort());
    }
    return this;
  }

  public ElasticDataPointerAssert hasIndexName(String indexName) {
    isNotNull();
    if (!actual.getIndexName().equals(indexName)) {
      failWithMessage("Expected character's indexName to be <%s> but was <%s>", indexName, actual.getIndexName());
    }
    return this;
  }

  public ElasticDataPointerAssert hasTypeName(String typeName) {
    isNotNull();
    if (!actual.getTypeName().equals(typeName)) {
      failWithMessage("Expected character's typeName to be <%s> but was <%s>", typeName, actual.getTypeName());
    }
    return this;
  }

  public ElasticDataPointerAssert hasClusterName(String clusterName) {
    isNotNull();
    if (!actual.getClusterName().equals(clusterName)) {
      failWithMessage("Expected character's clusterName to be <%s> but was <%s>", clusterName, actual.getClusterName());
    }
    return this;
  }

}
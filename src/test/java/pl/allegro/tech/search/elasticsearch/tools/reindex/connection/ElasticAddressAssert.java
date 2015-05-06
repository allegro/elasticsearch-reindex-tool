package pl.allegro.tech.search.elasticsearch.tools.reindex.connection;

import junit.framework.TestCase;
import org.assertj.core.api.AbstractAssert;
import pl.allegro.tech.search.elasticsearch.tools.reindex.query.PrefixSegment;

public class ElasticAddressAssert  extends AbstractAssert<ElasticAddressAssert, ElasticAddress> {

  protected ElasticAddressAssert(ElasticAddress actual) {
    super(actual, ElasticAddressAssert.class);
  }

  public static ElasticAddressAssert assertThat(ElasticAddress actual) {
    return new ElasticAddressAssert(actual);
  }

  public ElasticAddressAssert hasHost(String host) {
    isNotNull();
    if (!actual.getHost().equals(host)) {
      failWithMessage("Expected character's host to be <%s> but was <%s>", host, actual.getHost());
    }
    return this;
  }

  public ElasticAddressAssert hasPort(int port) {
    isNotNull();
    if (actual.getPort() != port) {
      failWithMessage("Expected port to be <%d> but was <%d>", port, actual.getPort());
    }
    return this;
  }

  public ElasticAddressAssert hasIndexName(String indexName) {
    isNotNull();
    if (!actual.getIndexName().equals(indexName)) {
      failWithMessage("Expected character's indexName to be <%s> but was <%s>", indexName, actual.getIndexName());
    }
    return this;
  }

  public ElasticAddressAssert hasTypeName(String typeName) {
    isNotNull();
    if (!actual.getTypeName().equals(typeName)) {
      failWithMessage("Expected character's typeName to be <%s> but was <%s>", typeName, actual.getTypeName());
    }
    return this;
  }

}
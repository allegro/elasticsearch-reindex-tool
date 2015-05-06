package pl.allegro.tech.search.elasticsearch.tools.reindex.connection;

public class ElasticDataPointerBuilder {

  private ElasticAddressParser elasticAddressParser = new ElasticAddressParser();

  private String clusterName = "elasticsearch";
  private ElasticAddress address;

  private ElasticDataPointerBuilder() {
  }

  public ElasticDataPointerBuilder setAddress(String uri) {
    address = elasticAddressParser.parse(uri);
    return this;
  }

  public ElasticDataPointerBuilder setClusterName(String clusterName) {
    this.clusterName = clusterName;
    return this;
  }

  public ElasticDataPointer build() {
    return new ElasticDataPointer(address.getHost(), clusterName, address.getIndexName(), address.getTypeName(),
        address.getPort());
  }

  public static ElasticDataPointerBuilder builder() {
    return new ElasticDataPointerBuilder();
  }

}
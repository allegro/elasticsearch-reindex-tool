package pl.allegro.tech.search.elasticsearch.tools.reindex.connection;

public class ElasticDataPointer {

  private final String host;
  private final String clusterName;
  private final String indexName;
  private final String typeName;
  private final int port;
  private final boolean sniff;

  ElasticDataPointer(String host, String clusterName, String indexName, String typeName, int port, boolean sniff) {
    this.host = host;
    this.clusterName = clusterName;
    this.indexName = indexName;
    this.typeName = typeName;
    this.port = port;
    this.sniff = sniff;
  }

  public String getHost() {
    return host;
  }

  public String getClusterName() {
    return clusterName;
  }

  public String getIndexName() {
    return indexName;
  }

  public String getTypeName() {
    return typeName;
  }

  public int getPort() {
    return port;
  }

  public boolean isSniff() {
    return sniff;
  }
}

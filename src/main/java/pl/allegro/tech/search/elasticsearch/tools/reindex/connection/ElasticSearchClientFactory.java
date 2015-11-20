package pl.allegro.tech.search.elasticsearch.tools.reindex.connection;

import java.net.InetSocketAddress;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public final class ElasticSearchClientFactory {

  private ElasticSearchClientFactory() {
  }

  public static Client createClient(ElasticDataPointer elasticDataPointer) {
    Settings settings = Settings.settingsBuilder()
        .put("client.transport.sniff", elasticDataPointer.isSniff())
        .put(ClusterName.SETTING, elasticDataPointer.getClusterName())
        .build();
    TransportClient client = TransportClient.builder().settings(settings).build();
    client.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress(elasticDataPointer.getHost(), elasticDataPointer
        .getPort())));
    return client;
  }
}

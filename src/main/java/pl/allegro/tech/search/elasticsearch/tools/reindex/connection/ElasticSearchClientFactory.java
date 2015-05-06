package pl.allegro.tech.search.elasticsearch.tools.reindex.connection;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public final class ElasticSearchClientFactory {

  private ElasticSearchClientFactory() {
  }

  public static Client createClient(ElasticDataPointer elasticDataPointer) {
    Settings settings = ImmutableSettings.settingsBuilder()
        .put("client.transport.sniff", true)
        .put(ClusterName.SETTING, elasticDataPointer.getClusterName())
        .build();
    TransportClient client = new TransportClient(settings);
    client.addTransportAddress(new InetSocketTransportAddress(elasticDataPointer.getHost(), elasticDataPointer
        .getPort()));
    return client;
  }
}

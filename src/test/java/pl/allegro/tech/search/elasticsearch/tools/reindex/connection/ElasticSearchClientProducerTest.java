package pl.allegro.tech.search.elasticsearch.tools.reindex.connection;

import org.assertj.core.api.Assertions;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.junit.Assert.assertEquals;

public class ElasticSearchClientProducerTest {

  public static final String CLUSTER_NAME = "my_test_cluster";
  public static final String INDEX_NAME = "index";
  private Node dataNode;

  @Before
  public void setUp() throws Exception {
    dataNode = nodeBuilder().clusterName(CLUSTER_NAME).node();
  }

  @After
  public void tearDown() throws Exception {
    dataNode.close();
  }

  @Test
  public void validateCreatedLocalElasticClientWithProperClusterName() throws Exception {
    //given
    ElasticDataPointer dataPointer = ElasticDataPointerBuilder.builder()
        .setAddress("http://localhost:9300/"+INDEX_NAME+"/type")
        .setClusterName(CLUSTER_NAME)
        .build();
    //when
    Client client = ElasticSearchClientFactory.createClient(dataPointer);
    //then
    Assertions.assertThat(client.settings().get("cluster.name")).isEqualTo(CLUSTER_NAME);
    client.close();
  }

}

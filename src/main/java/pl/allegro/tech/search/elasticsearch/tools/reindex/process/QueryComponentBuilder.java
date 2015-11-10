package pl.allegro.tech.search.elasticsearch.tools.reindex.process;

import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticDataPointer;
import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticSearchQuery;
import pl.allegro.tech.search.elasticsearch.tools.reindex.query.BoundedSegment;
import org.elasticsearch.client.Client;

import java.util.Optional;

public final class QueryComponentBuilder {
  private Client client;
  private ElasticDataPointer dataPointer;
  private Optional<String> segmentationField = Optional.empty();
  private Optional<BoundedSegment> bound = Optional.empty();
  private ElasticSearchQuery query;

  private QueryComponentBuilder() {
  }

  public QueryComponentBuilder setClient(Client client) {
    this.client = client;
    return this;
  }

  public QueryComponentBuilder setDataPointer(ElasticDataPointer dataPointer) {
    this.dataPointer = dataPointer;
    return this;
  }

  public QueryComponentBuilder setSegmentationField(Optional<String> segmentationField) {
    this.segmentationField = segmentationField;
    return this;
  }

  public QueryComponentBuilder setBound(Optional<BoundedSegment> bound) {
    this.bound = bound;
    return this;
  }

  public QueryComponentBuilder setQuery(ElasticSearchQuery query) {
    this.query = query;
    return this;
  }


  public static QueryComponentBuilder builder() {
    return new QueryComponentBuilder();
  }

  public QueryComponent createQueryComponent() {
    return new QueryComponent(client, dataPointer, segmentationField, bound, query);
  }

}
package pl.allegro.tech.search.elasticsearch.tools.reindex.query;

import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticSearchQuery;

public class SegmentationQueryTrait {

  private final ElasticSearchQuery query;

  public SegmentationQueryTrait(ElasticSearchQuery query) {
    this.query = query;
  }

  public ElasticSearchQuery getQuery() {
    return query;
  }

}

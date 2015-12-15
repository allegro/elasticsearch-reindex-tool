package pl.allegro.tech.search.elasticsearch.tools.reindex.query;

import com.sun.org.apache.xpath.internal.operations.Bool;
import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticSearchQuery;

public class SegmentationQueryTrait {

  private final ElasticSearchQuery query;
  private final Boolean segmentationByShards;

  public SegmentationQueryTrait(ElasticSearchQuery query, Boolean segmentationByShards) {
    this.query = query;
    this.segmentationByShards = segmentationByShards;
  }

  public ElasticSearchQuery getQuery() {
    return query;
  }

  public Boolean getSegmentationByShards() {
    return this.segmentationByShards;
  }
}

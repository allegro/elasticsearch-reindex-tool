package pl.allegro.tech.search.elasticsearch.tools.reindex.query;

import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticSearchQuery;

import java.util.Optional;

public interface QuerySegmentation {

  Optional<String> getFieldName();

  int getSegmentsCount();

  Optional<BoundedSegment> getThreshold(int i);

  ElasticSearchQuery getQuery(int i);
}

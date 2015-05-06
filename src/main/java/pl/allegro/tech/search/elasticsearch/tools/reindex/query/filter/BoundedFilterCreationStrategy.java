package pl.allegro.tech.search.elasticsearch.tools.reindex.query.filter;

import pl.allegro.tech.search.elasticsearch.tools.reindex.query.BoundedSegment;
import org.elasticsearch.index.query.BaseQueryBuilder;

public interface BoundedFilterCreationStrategy<SegmentType> {

  BaseQueryBuilder create(String fieldName, SegmentType resolvedBound);

}

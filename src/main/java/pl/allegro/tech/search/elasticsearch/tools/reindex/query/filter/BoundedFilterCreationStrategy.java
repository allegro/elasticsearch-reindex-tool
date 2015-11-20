package pl.allegro.tech.search.elasticsearch.tools.reindex.query.filter;

import org.elasticsearch.index.query.QueryBuilder;

public interface BoundedFilterCreationStrategy<SegmentType> {

	QueryBuilder create(String fieldName, SegmentType resolvedBound);

}

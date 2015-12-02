package pl.allegro.tech.search.elasticsearch.tools.reindex.query.filter;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;

import pl.allegro.tech.search.elasticsearch.tools.reindex.query.RangeSegment;

public class RangeFilterCreationStrategy implements BoundedFilterCreationStrategy<RangeSegment> {
  @Override
  public QueryBuilder create(String fieldName, RangeSegment resolvedBound) {
    return new RangeQueryBuilder(fieldName)
        .lte(resolvedBound.getUpperBound())
        .gt(resolvedBound.getLowerOpenBound());
  }

}

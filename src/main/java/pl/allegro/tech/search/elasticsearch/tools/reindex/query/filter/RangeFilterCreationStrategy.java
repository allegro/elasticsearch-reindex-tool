package pl.allegro.tech.search.elasticsearch.tools.reindex.query.filter;

import pl.allegro.tech.search.elasticsearch.tools.reindex.query.BoundedSegment;
import pl.allegro.tech.search.elasticsearch.tools.reindex.query.RangeSegment;
import org.elasticsearch.index.query.BaseQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;

public class RangeFilterCreationStrategy implements BoundedFilterCreationStrategy<RangeSegment> {
  @Override
  public BaseQueryBuilder create(String fieldName, RangeSegment resolvedBound) {
    return new RangeQueryBuilder(fieldName)
        .lte(resolvedBound.getUpperBound())
        .gt(resolvedBound.getLowerOpenBound());
  }

}

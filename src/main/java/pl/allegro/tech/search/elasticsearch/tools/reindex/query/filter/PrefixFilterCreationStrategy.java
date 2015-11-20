package pl.allegro.tech.search.elasticsearch.tools.reindex.query.filter;

import org.elasticsearch.index.query.PrefixQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import pl.allegro.tech.search.elasticsearch.tools.reindex.query.PrefixSegment;

public class PrefixFilterCreationStrategy implements BoundedFilterCreationStrategy<PrefixSegment> {
  @Override
  public QueryBuilder create(String fieldName, PrefixSegment resolvedBound) {
    return new PrefixQueryBuilder(fieldName, resolvedBound.getPrefix());
  }

}

package pl.allegro.tech.search.elasticsearch.tools.reindex.query.filter;

import pl.allegro.tech.search.elasticsearch.tools.reindex.query.BoundedSegment;
import pl.allegro.tech.search.elasticsearch.tools.reindex.query.PrefixSegment;
import org.elasticsearch.index.query.BaseQueryBuilder;
import org.elasticsearch.index.query.PrefixQueryBuilder;

public class PrefixFilterCreationStrategy implements BoundedFilterCreationStrategy<PrefixSegment> {
  @Override
  public BaseQueryBuilder create(String fieldName, PrefixSegment resolvedBound) {
    return new PrefixQueryBuilder(fieldName, resolvedBound.getPrefix());
  }

}

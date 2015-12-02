package pl.allegro.tech.search.elasticsearch.tools.reindex.query.filter;

import static org.assertj.core.api.Assertions.assertThat;

import org.elasticsearch.index.query.PrefixQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.junit.Test;

import pl.allegro.tech.search.elasticsearch.tools.reindex.query.PrefixSegment;
import pl.allegro.tech.search.elasticsearch.tools.reindex.query.RangeSegment;

public class BoundedFilterFactoryTest {

  @Test
  public void shouldCreatePrefixFilter() throws Exception {
    //given
    BoundedFilterFactory factory = new BoundedFilterFactory();
    PrefixSegment anyPrefixSegment = new PrefixSegment("prefix");
    //when
    QueryBuilder filter = factory.createBoundedFilter("fieldName", anyPrefixSegment);
    //then
    assertThat(filter).isInstanceOf(PrefixQueryBuilder.class);
  }

  @Test
  public void shouldCreateDoubleBoundedFilter() throws Exception {
    //given
    BoundedFilterFactory factory = new BoundedFilterFactory();
    RangeSegment anyRangeSegment = new RangeSegment(1.0, 2.0);
    //when
    QueryBuilder filter = factory.createBoundedFilter("fieldName", anyRangeSegment);
    //then
    assertThat(filter).isInstanceOf(RangeQueryBuilder.class);
  }
}
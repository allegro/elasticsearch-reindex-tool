package pl.allegro.tech.search.elasticsearch.tools.reindex.connection;

import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import pl.allegro.tech.search.elasticsearch.tools.reindex.command.ReindexCommand;

/**
 * Used when starting the reindex from a specific point.
 */
public class ElasticSearchQuery {
  private final String query;
  private final SortBuilder sort;

  public ElasticSearchQuery(String query) {
   this(query, null);
  }

  public ElasticSearchQuery(String query, SortBuilder sort) {
    this.query = query;
    this.sort = sort;
  }

  public String getQuery() {
    return query;
  }

  public SortBuilder getSort() {
    return sort;
  }

  public static class ElasticSearchQueryBuilder {
    private String query;
    private String sort;
    private SortOrder sortOrder;
    public ElasticSearchQueryBuilder(ReindexCommand reindexCommand) {
      if (reindexCommand != null) {
        this.query = reindexCommand.getQuery();
        this.sort = reindexCommand.getSort();
        try {
          this.sortOrder = SortOrder.valueOf(reindexCommand.getSortOrder());
        } catch (IllegalArgumentException | NullPointerException e) {
          this.sortOrder = SortOrder.ASC;
        }
      }
    }

    public ElasticSearchQuery build() {
      return new ElasticSearchQuery(query, new FieldSortBuilder(sort).order(sortOrder));
    }
  }
}

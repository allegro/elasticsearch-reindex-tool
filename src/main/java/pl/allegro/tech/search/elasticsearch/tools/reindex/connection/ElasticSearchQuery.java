package pl.allegro.tech.search.elasticsearch.tools.reindex.connection;

import org.elasticsearch.search.sort.SortOrder;

public class ElasticSearchQuery {
  private final String query;
  private final String sortField;
  private final SortOrder sortOrder;

  ElasticSearchQuery(String query, String sortField, SortOrder sortOrder) {
    this.query = query;
    this.sortField = sortField;
    this.sortOrder = sortOrder;
  }

  public String getQuery() {
    return query;
  }

  public SortOrder getSortOrder() {
    return sortOrder;
  }

  public String getSortField() {
    return sortField;
  }
}

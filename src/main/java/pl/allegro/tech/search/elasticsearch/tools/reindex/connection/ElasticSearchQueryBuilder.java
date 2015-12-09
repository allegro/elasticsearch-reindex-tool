package pl.allegro.tech.search.elasticsearch.tools.reindex.connection;

import com.google.common.base.Strings;
import org.elasticsearch.search.sort.SortOrder;

public class ElasticSearchQueryBuilder {
  private String query;
  private String sortField;
  private SortOrder sortOrder = SortOrder.ASC;


  private ElasticSearchQueryBuilder() {
  }

  public ElasticSearchQueryBuilder setSortOrder(String sortOrder) {
    if (!Strings.isNullOrEmpty(sortOrder)) {
      try {
        this.sortOrder = SortOrder.valueOf(sortOrder);
      } catch (IllegalArgumentException e) {
        throw new ParsingElasticsearchAddressException("SortOrder can be only ASC or DESC, not " + sortOrder);
      }
    }
    return this;
  }

  public ElasticSearchQueryBuilder setQuery(String query) {
    this.query = query;
    return this;
  }

  public ElasticSearchQueryBuilder setSortByField(String orderByField) {
    this.sortField = orderByField;
    return this;
  }

  public ElasticSearchQuery build() {
    return new ElasticSearchQuery(query, sortField, sortOrder);
  }

  public static ElasticSearchQueryBuilder builder() {
    return new ElasticSearchQueryBuilder();
  }


}

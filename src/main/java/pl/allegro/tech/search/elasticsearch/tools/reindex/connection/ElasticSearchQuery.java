package pl.allegro.tech.search.elasticsearch.tools.reindex.connection;

import org.apache.commons.collections4.ArrayStack;
import org.elasticsearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.StreamSupport;

public class ElasticSearchQuery {
  private final String query;
  private final String sortField;
  private final SortOrder sortOrder;
  private final List<Integer> shards;

  ElasticSearchQuery(String query, String sortField, SortOrder sortOrder, List<Integer> shards) {
    this.query = query;
    this.sortField = sortField;
    this.sortOrder = sortOrder;
    this.shards = Collections.unmodifiableList(shards);
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

  public List<Integer> getShards() {
    return this.shards;
  }

  public ElasticSearchQuery withShards(List<Integer> shards) {
    return new ElasticSearchQuery(query, sortField, sortOrder, Collections.unmodifiableList(shards));
  }
}

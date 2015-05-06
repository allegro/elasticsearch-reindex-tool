package pl.allegro.tech.search.elasticsearch.tools.reindex.embeded;

import java.util.Map;

public class IndexDocument {
  private final String id;
  private final Map<String, ?> doc;

  public IndexDocument(String id, Map<String, ?> doc) {
    this.id = id;
    this.doc = doc;
  }

  public String getId() {
    return id;
  }

  public Map<String, ?> getDoc() {
    return doc;
  }
}

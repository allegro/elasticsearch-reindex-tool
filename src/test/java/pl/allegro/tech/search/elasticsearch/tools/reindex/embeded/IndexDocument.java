package pl.allegro.tech.search.elasticsearch.tools.reindex.embeded;

import java.util.Map;

public class IndexDocument {
  private final String id;
  private final Map<String, Object> doc;
  private final Long ttl;

  public IndexDocument(String id, Map<String, Object> doc, Long ttl) {
    this.id = id;
    this.doc = doc;
    this.ttl = ttl != null && ttl > 0 ? ttl : null;
  }

  public IndexDocument(String id, Map<String, Object> doc) {
    this(id, doc, null);
  }

  public String getId() {
    return id;
  }

  public Map<String, Object> getDoc() {
    return doc;
  }

  public Long getTTL() {
    return ttl;
  }
}

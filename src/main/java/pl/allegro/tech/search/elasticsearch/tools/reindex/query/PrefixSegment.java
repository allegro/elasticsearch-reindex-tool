package pl.allegro.tech.search.elasticsearch.tools.reindex.query;

public class PrefixSegment implements BoundedSegment {

  private final String prefix;

  public PrefixSegment(String prefix) {
    this.prefix = prefix;
  }

  public String getPrefix() {
    return prefix;
  }
}

package pl.allegro.tech.search.elasticsearch.tools.reindex.connection;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ElasticAddressParser {

  public static final Pattern URI_PATTERN = Pattern.compile("http://([^:]+):(\\d+)/([^/]+)/([^/]+)$");

  public ElasticAddress parse(String uri) {
    Matcher matcher = URI_PATTERN.matcher(uri);
    ElasticAddress elasticAddress = new ElasticAddress();
    if (matcher.find()) {
      elasticAddress.setHost(matcher.group(1));
      elasticAddress.setPort(Integer.parseInt(matcher.group(2)));
      elasticAddress.setIndexName(matcher.group(3));
      elasticAddress.setTypeName(matcher.group(4));
      return elasticAddress;
    } else {
      throw new ParsingElasticsearchAddressException("Could not parse elasticsearch url: " + uri);
    }
  }
}

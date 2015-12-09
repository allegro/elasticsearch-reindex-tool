package pl.allegro.tech.search.elasticsearch.tools.reindex.query;

import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticSearchQuery;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class StringPrefixSegmentation extends SegmentationQueryTrait implements QuerySegmentation {

  private final String fieldName;

  private final List<PrefixSegment> prefixSegmentsList;

  public StringPrefixSegmentation(String fieldName, List<String> prefixesList, ElasticSearchQuery query) {
    super(query);
    this.fieldName = fieldName;
    this.prefixSegmentsList = Collections.unmodifiableList(
        prefixesList.stream().map(PrefixSegment::new).collect(Collectors.toList()));
  }

  @Override
  public Optional<String> getFieldName() {
    return Optional.of(fieldName);
  }

  @Override
  public int getSegmentsCount() {
    return prefixSegmentsList.size();
  }

  @Override
  public Optional<BoundedSegment> getThreshold(int i) {
    return Optional.of(prefixSegmentsList.get(i));
  }

  public static QuerySegmentation create(String fieldName, List<String> segmentationPrefixes, ElasticSearchQuery query) {
    return new StringPrefixSegmentation(fieldName, segmentationPrefixes, query);
  }
}

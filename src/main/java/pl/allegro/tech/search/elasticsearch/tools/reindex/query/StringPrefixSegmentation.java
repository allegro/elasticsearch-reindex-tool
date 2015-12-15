package pl.allegro.tech.search.elasticsearch.tools.reindex.query;

import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticSearchQuery;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class StringPrefixSegmentation extends SegmentationQueryTrait implements QuerySegmentation {

  private final String fieldName;

  private final List<PrefixSegment> prefixSegmentsList;

  public StringPrefixSegmentation(String fieldName, List<String> prefixesList, ElasticSearchQuery query, Boolean segmentationByShards) {
    super(query, segmentationByShards);
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
    if (this.getSegmentationByShards() && this.getQuery().getShards().size() > 1) {
      return prefixSegmentsList.size() * this.getQuery().getShards().size();
    }
    return prefixSegmentsList.size();
  }

  @Override
  public Optional<BoundedSegment> getThreshold(int i) {
    int thresholdIndex = i;
    if (getSegmentationByShards() && this.getQuery().getShards().size() > 1) {
      thresholdIndex = i % prefixSegmentsList.size();
    }

    return Optional.of(prefixSegmentsList.get(thresholdIndex));
  }

  @Override
  public ElasticSearchQuery getQuery(int i) {
    if (getSegmentationByShards() && this.getQuery().getShards().size() > 1) {
      return getQuery().withShards(Arrays.asList(getQuery().getShards().get(i / prefixSegmentsList.size())));
    }

    return getQuery();
  }

  public static StringPrefixSegmentation create(String fieldName, List<String> segmentationPrefixes, ElasticSearchQuery query, Boolean segmentationByShards) {
    return new StringPrefixSegmentation(fieldName, segmentationPrefixes, query, segmentationByShards);
  }
}

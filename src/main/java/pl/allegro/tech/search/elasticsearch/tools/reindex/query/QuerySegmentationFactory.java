package pl.allegro.tech.search.elasticsearch.tools.reindex.query;

import pl.allegro.tech.search.elasticsearch.tools.reindex.command.ReindexCommand;
import org.apache.commons.collections4.CollectionUtils;

public final class QuerySegmentationFactory {

  private QuerySegmentationFactory() {
  }

  public static QuerySegmentation create(ReindexCommand command) {
    if (command.getSegmentationField() == null) {
      return EmptySegmentation.createEmptySegmentation();
    }
    if (CollectionUtils.isNotEmpty(command.getSegmentationThresholds())) {
      return DoubleFieldSegmentation.create(command.getSegmentationField(), command.getSegmentationThresholds());
    }
    if (CollectionUtils.isNotEmpty(command.getSegmentationPrefixes())) {
      return StringPrefixSegmentation.create(command.getSegmentationField(), command.getSegmentationPrefixes());
    }
    throw new BadSegmentationDefinitionException("Bad segmentation creation params");
  }
}

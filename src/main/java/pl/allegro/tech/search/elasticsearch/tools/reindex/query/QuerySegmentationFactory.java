package pl.allegro.tech.search.elasticsearch.tools.reindex.query;

import pl.allegro.tech.search.elasticsearch.tools.reindex.command.ReindexCommand;
import org.apache.commons.collections4.CollectionUtils;
import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticSearchQuery;
import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticSearchQueryBuilder;

public final class QuerySegmentationFactory {

  private QuerySegmentationFactory() {
  }

  public static QuerySegmentation create(ReindexCommand command) {
    ElasticSearchQuery query = buildQuery(command);

    if (command.getSegmentationByShards() && query.getShards().isEmpty()) {
      throw new BadSegmentationDefinitionException("Bad segmentation creation params");
    }

    if (command.getSegmentationField() == null) {
      if (command.getSegmentationByShards()) {
        return OnlyShardSegmentation.createOnlyShardsSegmentation(query);
      }
      return EmptySegmentation.createEmptySegmentation(query);
    }

    if (CollectionUtils.isNotEmpty(command.getSegmentationThresholds())) {
      return DoubleFieldSegmentation.create(command.getSegmentationField(), command.getSegmentationThresholds(), query, command.getSegmentationByShards());
    }

    if (CollectionUtils.isNotEmpty(command.getSegmentationPrefixes())) {
      return StringPrefixSegmentation.create(command.getSegmentationField(), command.getSegmentationPrefixes(), query, command.getSegmentationByShards());
    }
    throw new BadSegmentationDefinitionException("Bad segmentation creation params");
  }

  private static ElasticSearchQuery buildQuery(ReindexCommand command) {
    return ElasticSearchQueryBuilder.builder()
            .setQuery(command.getQuery())
            .setSortByField(command.getSort())
            .setSortOrder(command.getSortOrder())
            .setShards(command.getShards())
            .build();
  }
}

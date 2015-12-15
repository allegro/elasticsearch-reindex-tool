package pl.allegro.tech.search.elasticsearch.tools.reindex.query;

import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticSearchQuery;

import java.util.Arrays;
import java.util.Optional;

/**
 * Created by Roman on 12/14/2015.
 */
public class OnlyShardSegmentation extends SegmentationQueryTrait implements QuerySegmentation {

    private OnlyShardSegmentation(ElasticSearchQuery query) {
        super(query, true);
    }

    @Override
    public Optional<String> getFieldName() {
        return Optional.empty();
    }

    @Override
    public int getSegmentsCount() {
        if (this.getQuery().getShards().size() > 1) {
            return this.getQuery().getShards().size();
        }

        return 1;
    }

    @Override
    public Optional<BoundedSegment> getThreshold(int i) {
        return Optional.empty();
    }

    @Override
    public ElasticSearchQuery getQuery(int i) {
        if (this.getQuery().getShards().size() > 1) {
            return this.getQuery().withShards(Arrays.asList(this.getQuery().getShards().get(i)));
        }

        return this.getQuery();
    }

    public static OnlyShardSegmentation createOnlyShardsSegmentation(ElasticSearchQuery query) {
        return new OnlyShardSegmentation(query);
    }
}

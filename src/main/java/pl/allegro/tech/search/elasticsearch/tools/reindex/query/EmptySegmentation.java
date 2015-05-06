package pl.allegro.tech.search.elasticsearch.tools.reindex.query;

import java.util.Optional;

public final class EmptySegmentation implements QuerySegmentation {

  public static final EmptySegmentation EMPTY_SEGMENTATION_INSTANCE = new EmptySegmentation();

  private EmptySegmentation() {
  }

  @Override
  public Optional<String> getFieldName() {
    return Optional.empty();
  }

  @Override
  public int getSegmentsCount() {
    return 1;
  }

  @Override
  public Optional<BoundedSegment> getThreshold(int i) {
    return Optional.empty();
  }

  public static EmptySegmentation createEmptySegmentation() {
    return EMPTY_SEGMENTATION_INSTANCE;
  }

}

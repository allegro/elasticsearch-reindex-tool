package pl.allegro.tech.search.elasticsearch.tools.reindex.command;

import com.beust.jcommander.Parameter;

import java.util.Collections;
import java.util.List;

public class ReindexCommand {

  @Parameter(names = { "-s", "source" }, description = "Source f.e. http://localhost:9300/source_index/type",
          required = true)
  private String source;

  @Parameter(names = { "-sc", "source-cluster" }, description = "Source cluster name", required = true)
  private String sourceClusterName;

  @Parameter(names = { "-tc", "target-cluster" }, description = "Target cluster name", required = true)
  private String targetClusterName;
  @Parameter(names = { "-t", "target" }, description = "Target f.e. http://localhost:9300/target_index/type",
          required = true)
  private String target;

  @Parameter(names = { "-segmentationField" }, description = "Segmentation field")
  private String segmentationField;

  @Parameter(names = { "-query" }, description = "Give a query to filter data")
  private String query;

  @Parameter(names = { "-shards" }, description = "Select the shards that will accept the query")
  private List<Integer> shards = Collections.emptyList();

  @Parameter(names = { "-sort" }, description = "Give field to sort on (if query option in use)")
  private String sort;

  @Parameter(names = { "-sortOrder" }, description = "Give sortOrder (if query option in use)")
  private String sortOrder;

  @Parameter(names = { "-segmentationThresholds" }, description = "Segmentation thresholds (only double type)")
  private List<Double> segmentationThresholds;

  @Parameter(names = { "-segmentationPrefixes" }, description = "Segmentation prefixes (comma-separated)")
  private List<String> segmentationPrefixes;

  @Parameter(names = { "-segmentationByShards" }, description = "Segmentation by shards (true/false)")
  private boolean segmentationByShards = false;

  public String getSourceClusterName() {
    return sourceClusterName;
  }
  public String getTargetClusterName() {
    return targetClusterName;
  }
  public String getSegmentationField() {
    return segmentationField;
  }

  public String getQuery() {
    return query;
  }

  public List<Integer> getShards() {
    return this.shards;
  }

  public String getSort() {
    return sort;
  }

  public String getSortOrder() {
    return sortOrder;
  }

  public List<Double> getSegmentationThresholds() {
    return segmentationThresholds;
  }

  public List<String> getSegmentationPrefixes() {
    return segmentationPrefixes;
  }

  public boolean getSegmentationByShards() { return segmentationByShards; }

  public String getSource() {
    return source;
  }

  public String getTarget() {
    return target;
  }
}

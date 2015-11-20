package pl.allegro.tech.search.elasticsearch.tools.reindex;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

import pl.allegro.tech.search.elasticsearch.tools.reindex.command.ReindexCommand;
import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticDataPointer;
import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticDataPointerBuilder;
import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ParsingElasticsearchAddressException;
import pl.allegro.tech.search.elasticsearch.tools.reindex.query.QuerySegmentation;
import pl.allegro.tech.search.elasticsearch.tools.reindex.query.QuerySegmentationFactory;

public class ReindexCommandParser {

  private ElasticDataPointer sourcePointer;
  private ElasticDataPointer targetPointer;
  private QuerySegmentation segmentation;


  public boolean tryParse(String... args) {
    ReindexCommand command = new ReindexCommand();
    JCommander jCommander = new JCommander(command);

    try {
      jCommander.parse(args);
      buildReindexParameters(command);

    } catch (ParameterException | ParsingElasticsearchAddressException exception) {
      JCommander.getConsole().println("Parameters error occurred:");
      JCommander.getConsole().println(exception.getMessage());
      JCommander.getConsole().println("");

      jCommander.usage();
      return false;
    }
    return true;
  }

  private void buildReindexParameters(ReindexCommand command) {
    sourcePointer = ElasticDataPointerBuilder.builder()
        .setClusterName(command.getSourceClusterName())
        .setAddress(command.getSource())
        .setSniff(!command.isDisableSniff())
        .build();
    targetPointer = ElasticDataPointerBuilder.builder()
        .setClusterName(command.getTargetClusterName())
        .setAddress(command.getTarget())
        .setSniff(!command.isDisableSniff())
        .build();
    segmentation = getFieldSegmentation(command);
  }

  private QuerySegmentation getFieldSegmentation(ReindexCommand command) {
    return QuerySegmentationFactory.create(command);
  }

  public ElasticDataPointer getSourcePointer() {
    return sourcePointer;
  }

  public ElasticDataPointer getTargetPointer() {
    return targetPointer;
  }

  public QuerySegmentation getSegmentation() {
    return segmentation;
  }
}

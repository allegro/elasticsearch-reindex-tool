package pl.allegro.tech.search.elasticsearch.tools.reindex;

public final class ReindexAction {

  private ReindexAction() {
    throw new IllegalAccessError();
  }

  public static void main(String[] args) {
    ReindexCommandParser commandParser = new ReindexCommandParser();
    if (commandParser.tryParse(args)) {
      ReindexInvoker.invokeReindexing(
          commandParser.getSourcePointer(),
          commandParser.getTargetPointer(),
          commandParser.getSegmentation(),
          commandParser.getQuery());
    }
  }
}

package pl.allegro.tech.search.elasticsearch.tools.reindex.process;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ProcessExecutor {

  private static final Logger logger = LoggerFactory.getLogger(ProcessExecutor.class);

  private ExecutorService executorService;

  public ProcessExecutor(int queryThreadsCount) {
    this.executorService = Executors.newFixedThreadPool(queryThreadsCount + ProcessConfiguration.getInstance().getUpdateThreadsCount());
  }

  public void startProcess(Runnable process) {
    executorService.submit(process);
  }

  public void finishProcessing() {
    try {
      executorService.shutdown();
      executorService.awaitTermination(1, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      logger.error("Closing executor service failed");
      Thread.currentThread().interrupt();
    }
  }

}

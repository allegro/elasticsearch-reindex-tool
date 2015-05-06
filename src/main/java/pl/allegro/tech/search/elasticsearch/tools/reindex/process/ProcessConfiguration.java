package pl.allegro.tech.search.elasticsearch.tools.reindex.process;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ProcessConfiguration {

  private static final String PROPERTIES_FILE_NAME = "config.properties";
  private static final String INDEXING_QUEUE_SIZE = "indexing.queue.size";
  private static final String UPDATE_THREADS_COUNT = "update.threads.count";
  private static final String QUEUE_POLL_TIMEOUT = "queue.poll.timeout";
  private static final String QUEUE_OFFER_TIMEOUT = "queue.offer.timeout";

  private final int queueSize;
  private final int updateThreadsCount;
  private final int queuePollTimeout;
  private final int queueOfferTimeout;

  public ProcessConfiguration() {
    InputStream inputStream = getClass().getClassLoader().getResourceAsStream(PROPERTIES_FILE_NAME);
    Properties prop = new Properties();
    try {
      prop.load(inputStream);
      queueSize = Integer.parseInt(prop.getProperty(INDEXING_QUEUE_SIZE));
      updateThreadsCount = Integer.parseInt(prop.getProperty(UPDATE_THREADS_COUNT));
      queueOfferTimeout = Integer.parseInt(prop.getProperty(QUEUE_OFFER_TIMEOUT));
      queuePollTimeout = Integer.parseInt(prop.getProperty(QUEUE_POLL_TIMEOUT));
    } catch (IOException | NumberFormatException e) {
      throw new RuntimeException("Failed reading config file: " + PROPERTIES_FILE_NAME, e);
    }
  }

  public int getQueueSize() {
    return queueSize;
  }

  public int getUpdateThreadsCount() {
    return updateThreadsCount;
  }

  public int getQueuePollTimeout() {
    return queuePollTimeout;
  }

  public int getQueueOfferTimeout() {
    return queueOfferTimeout;
  }

  private static ProcessConfiguration instance;

  public static synchronized ProcessConfiguration getInstance() {
    if (instance == null) {
      instance = new ProcessConfiguration();
    }
    return instance;
  }

  public static void main(String[] args) {
    new ProcessConfiguration();
  }
}

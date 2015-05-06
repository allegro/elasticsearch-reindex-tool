package pl.allegro.tech.search.elasticsearch.tools.reindex.process;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

public class ProcessExecutorTest {

  @Test
  public void testExecutingAndEndingProcess() throws Exception {
    //given
    int queryThreadsAmount = 3;
    ProcessExecutor processExecutor = new ProcessExecutor(queryThreadsAmount);
    AtomicBoolean executed = new AtomicBoolean();
    //when
    processExecutor.startProcess(() -> executed.set(true));
    processExecutor.finishProcessing();
    //then
    Assert.assertTrue(executed.get());
  }
}
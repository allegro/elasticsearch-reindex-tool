package pl.allegro.tech.search.elasticsearch.tools.reindex.process;

import com.beust.jcommander.internal.Lists;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.Test;
import org.mockito.Mockito;
import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticDataPointer;
import pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticDataPointerBuilder;

import java.util.Collections;
import java.util.Optional;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IndexingProcessTest {

  private static final String INDEX = "index";
  private static final String TYPE = "type";

  @Test
  public void verifyIndexingProcessDoNotIndexWhenNoDataToIndex() throws Exception {
    //given
    ProcessSynchronizer processSynchronizer = mock(ProcessSynchronizer.class);
    when(processSynchronizer.hasDataToBeIndexed()).thenReturn(false);
    IndexingComponent indexingComponent = mock(IndexingComponent.class);

    //when
    IndexingProcess updatesProcess = IndexingProcessBuilder.builder()
        .setProcessSynchronizer(processSynchronizer)
        .setIndexingComponent(indexingComponent)
        .build();
    updatesProcess.run();

    //then
    verify(processSynchronizer, times(0)).incrementUpdates(Mockito.anyInt());
  }

  @Test
  public void verifyIndexedCount() throws Exception {
    //given
    ProcessSynchronizer processSynchronizer = buildProcessSynchronizerMock();
    ElasticDataPointer dataPointer = ElasticDataPointerBuilder
        .builder()
        .setAddress("http://localhost:9300/" + INDEX + "/" + TYPE)
        .build();
    IndexingComponent indexingComponent = mock(IndexingComponent.class);
    when(indexingComponent.indexData(eq(dataPointer), any(SearchHit[].class)))
        .thenReturn(Optional.of(new BulkResult(4, Collections.emptyList())));

    //when
    IndexingProcess updatesProcess = IndexingProcessBuilder.builder()
        .setProcessSynchronizer(processSynchronizer)
        .setIndexingComponent(indexingComponent)
        .setDataPointer(dataPointer)
        .build();
    updatesProcess.run();

    //then
    verify(processSynchronizer).incrementUpdates(4);
  }

  @Test
  public void verifyIndexedFailedCount() throws Exception {

    //given
    ElasticDataPointer dataPointer = ElasticDataPointerBuilder.builder()
        .setAddress("http://localhost:9300/" + INDEX + "/" + TYPE)
        .build();
    ProcessSynchronizer processSynchronizer = buildProcessSynchronizerMock();
    IndexingComponent indexingComponent = mock(IndexingComponent.class);
    when(indexingComponent.indexData(eq(dataPointer), any(SearchHit[].class)))
        .thenReturn(Optional.of(new BulkResult(0, Lists.newArrayList("1", "2"))));

    //when
    IndexingProcess updatesProcess = IndexingProcessBuilder.builder()
        .setProcessSynchronizer(processSynchronizer)
        .setIndexingComponent(indexingComponent)
        .setDataPointer(dataPointer)
        .build();
    updatesProcess.run();

    //then
    verify(processSynchronizer, times(1)).incrementFailures(2);
  }

  private ProcessSynchronizer buildProcessSynchronizerMock() throws Exception {
    ProcessSynchronizer processSynchronizer = mock(ProcessSynchronizer.class);
    when(processSynchronizer.hasDataToBeIndexed()).thenReturn(true, false);
    SearchHits searchHits = mock(SearchHits.class);
    when(processSynchronizer.pollDataToIndexed()).thenReturn(searchHits);
    return processSynchronizer;
  }

}
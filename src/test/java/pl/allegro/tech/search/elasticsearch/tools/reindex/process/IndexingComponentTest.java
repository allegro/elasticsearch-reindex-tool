package pl.allegro.tech.search.elasticsearch.tools.reindex.process;

import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class IndexingComponentTest {

  public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm");

  @Test
  public void testComputeIndexName() throws Exception {
    // no replacement and error cases
    assertEquals("myindex", IndexingComponent.computeIndexName("myindex", null, null));
    assertEquals("", IndexingComponent.computeIndexName("", null, null));
    try {
      IndexingComponent.computeIndexName("${value}", Collections.emptyMap(), null);
      fail("Should throw NullPointerException with informative text");
    } catch (NullPointerException e) {
      assertTrue(!e.getMessage().isEmpty());
    }

    // simple replacement
    assertEquals("123", IndexingComponent.computeIndexName("${value}", Collections.singletonMap("value", "123"), null));
    assertEquals("doc-123", IndexingComponent.computeIndexName("doc-${value}", Collections.singletonMap("value", "123"), null));
    assertEquals("doc-123doc", IndexingComponent.computeIndexName("doc-${value}doc", Collections.singletonMap("value", "123"), null));

    // multiple replacements
    Map<String, Object> map = new HashMap<>();
    map.put("value", (Object) "123");
    map.put("key", (Object) "key45");
    assertEquals("doc-123dockey45", IndexingComponent.computeIndexName("doc-${value}doc${key}", map, null));

    // source-index
    assertEquals("doc-idx43doc", IndexingComponent.computeIndexName("doc-${_index}doc", null, "idx43"));

    // parse the date to avoid issues due to different timezones
    long jan1970 = DATE_FORMAT.parse("1970-01-01 03:23").getTime();
    long may2015 = DATE_FORMAT.parse("2015-05-23 11:23").getTime();

    // date/time formatting
    assertEquals("doc-1970-01-01doc", IndexingComponent.computeIndexName("doc-${startTime:yyyy-MM-dd}doc", Collections.singletonMap("startTime", Long.toString(jan1970)), null));
    assertEquals("doc-2015-05-23doc", IndexingComponent.computeIndexName("doc-${startTime:yyyy-MM-dd}doc", Collections.singletonMap("startTime", Long.toString(may2015)), null));
    assertEquals("doc-2015-05-23 11:23:00doc", IndexingComponent.computeIndexName("doc-${startTime:yyyy-MM-dd HH:mm:ss}doc", Collections.singletonMap("startTime", Long.toString(may2015)), null));
    map.put("startTime", "1432332000000");
    map.put("endTime", "1456265552000");
    assertEquals("doc-2015-05-2323:12doc", IndexingComponent.computeIndexName("doc-${startTime:yyyy-MM-dd}${endTime:HH:mm}doc", map, null));
  }
}
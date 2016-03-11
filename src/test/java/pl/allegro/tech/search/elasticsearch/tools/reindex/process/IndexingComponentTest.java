package pl.allegro.tech.search.elasticsearch.tools.reindex.process;

import org.elasticsearch.common.collect.MapBuilder;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class IndexingComponentTest {

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
        map.put("value", (Object)"123");
        map.put("key", (Object)"key45");
        assertEquals("doc-123dockey45", IndexingComponent.computeIndexName("doc-${value}doc${key}", map, null));

        // source-index
        assertEquals("doc-idx43doc", IndexingComponent.computeIndexName("doc-${_index}doc", null, "idx43"));

        // date/time formatting
        assertEquals("doc-1970-01-01doc", IndexingComponent.computeIndexName("doc-${startTime:yyyy-MM-dd}doc", Collections.singletonMap("startTime", "2387433"), null));
        assertEquals("doc-2015-05-23doc", IndexingComponent.computeIndexName("doc-${startTime:yyyy-MM-dd}doc", Collections.singletonMap("startTime", "1432332000000"), null));
        map.put("startTime", "1432332000000");
        map.put("endTime", "1456265552000");
        assertEquals("doc-2015-05-2323:12doc", IndexingComponent.computeIndexName("doc-${startTime:yyyy-MM-dd}${endTime:HH:mm}doc", map, null));
    }
}
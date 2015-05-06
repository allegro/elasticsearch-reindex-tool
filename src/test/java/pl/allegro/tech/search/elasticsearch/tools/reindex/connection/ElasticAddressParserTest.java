package pl.allegro.tech.search.elasticsearch.tools.reindex.connection;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.googlecode.catchexception.CatchException.catchException;
import static com.googlecode.catchexception.CatchException.caughtException;
import static pl.allegro.tech.search.elasticsearch.tools.reindex.connection.ElasticAddressAssert.assertThat;

@RunWith(JUnitParamsRunner.class)
public class ElasticAddressParserTest {

  private ElasticAddressParser elasticAddressParser = new ElasticAddressParser();

  @Test
  public void shouldBuildForProperAddress() throws Exception {
    //given
    String uri = "http://host:123/index/type";
    //when
    ElasticAddress address = elasticAddressParser.parse(uri);
    //then
    assertThat(address)
        .hasHost("host")
        .hasPort(123)
        .hasIndexName("index")
        .hasTypeName("type");
  }

  @Test
  @Parameters({
      "http://host:123/index",
      "http://host/index",
      "host:123/index",
      "http://:123/index" })
  public void shouldNotBuildForAddressWithoutType(String uri) throws Exception {
    //when
    catchException(elasticAddressParser).parse(uri);
    //then
    Assertions.assertThat((Throwable) caughtException())
        .isInstanceOf(ParsingElasticsearchAddressException.class);
  }

}
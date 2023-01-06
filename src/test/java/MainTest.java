import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.XML;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import org.testcontainers.containers.PostgisContainerProvider;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import me.callsen.taylor.osm2graph_geoserver.Config;
import me.callsen.taylor.osm2graph_geoserver.Main;
import me.callsen.taylor.osm2graph_geoserver.data.GeoServerRestApi;
import me.callsen.taylor.osm2graph_geoserver.data.GraphDb;
import me.callsen.taylor.osm2graph_geoserver.data.PostgisDb;
import me.callsen.taylor.osm2graph_geoserver.lib.HttpClient;

@Testcontainers
@TestInstance(Lifecycle.PER_CLASS)
@ExtendWith(MockitoExtension.class)
public class MainTest {
  
  private static PostgisContainerProvider postgisContainerProvider = new PostgisContainerProvider();

  // will be started before and stopped after each test method
  @Container
  private static final PostgreSQLContainer postgresqlContainer = (PostgreSQLContainer) postgisContainerProvider.newInstance()
      .withUsername("osm")
      .withPassword("osm")
      .withDatabaseName("osm")
      .withExposedPorts(5432);

  @TempDir
  private static Path tempDirectory;

  private Config appConfig;

  private GraphDb graphDb;

  private PostgisDb postgisDb;

  private GeoServerRestApi geoServer;

  @Mock
  private HttpClient httpClient;

  ArgumentCaptor<String> urlCaptor = ArgumentCaptor.forClass(String.class);

  ArgumentCaptor<String> authHeaderCaptor = ArgumentCaptor.forClass(String.class);

  ArgumentCaptor<String> postBodyCaptor = ArgumentCaptor.forClass(String.class);

  ArgumentCaptor<String> contentTypeCaptor = ArgumentCaptor.forClass(String.class);

  @BeforeAll
  public void initResources() throws Exception {
    ClassLoader classLoader = getClass().getClassLoader();
    MockitoAnnotations.openMocks(this);

    // copy graph db from resource into temporary (to avoid persisting changes)
    FileUtils.copyDirectory(new File(classLoader.getResource("neo4j/graph.db").getFile()), tempDirectory.toFile());

    // mock config object
    String testConfigString = Files.readString(Paths.get(classLoader.getResource("json/test-config.json").getFile()));
    JSONObject configObject = new JSONObject(testConfigString);

    // overlay postgis details from testcontainer
    JSONObject postgisConfig = new JSONObject();
    postgisConfig.put("host", postgresqlContainer.getHost());
    postgisConfig.put("port", Integer.toString(postgresqlContainer.getMappedPort(5432))); // https://stackoverflow.com/questions/58150827/cant-connect-to-a-testcontainer-postgres-instance
    postgisConfig.put("database", "osm"); // testcontainers loads all data into the test database..
    postgisConfig.put("user", "osm");
    postgisConfig.put("password", "osm");
    postgisConfig.put("schema", "test_sfpotrero");
    configObject.put("postgis", postgisConfig);

    // set graphdb temp location into config
    configObject.put("graphDbLocation", tempDirectory.toFile().getAbsolutePath());

    appConfig = new Config(configObject);

    graphDb = new GraphDb(appConfig.getString("graphDbLocation"));

    postgisDb = new PostgisDb(appConfig, postgresqlContainer.getJdbcUrl());

    // pass mocked httpClient into GeoServerRestApi to intercept & confirm GeoServer calls
    geoServer = new GeoServerRestApi(appConfig, httpClient);

    // execute load logic with testing app config - tests below confirm this processing
    Main.loadGeoServerData(appConfig, graphDb, postgisDb, geoServer);

    // populate arguement captors with arguments from mock calls
    verify(httpClient, times(3)).invokePost(urlCaptor.capture(), authHeaderCaptor.capture(), postBodyCaptor.capture(), contentTypeCaptor.capture());
  }

  @Test
  public void testPostgisVisualizationTableRowCount() throws Exception {
    List<Map<String,Object>> result = executePostgisQuery("SELECT COUNT(*) FROM test_sfpotrero.sfpotrero_vis_osm_landusages");
    assertEquals(98, (long)result.get(0).get("count"));
  }

  @Test
  public void testPostgisVisualizationTableRowProperties() throws Exception {
    List<Map<String,Object>> result = executePostgisQuery("SELECT osm_id, ST_AsText(geom) as geom, \"associatedData\"::Text, \"relationshipData\"::Text FROM test_sfpotrero.sfpotrero_vis_osm_landusages WHERE id=40");
    
    assertEquals(306786518, (long) result.get(0).get("osm_id"));
    assertEquals("LINESTRING(-122.399779 37.753165,-122.3995458 37.7530642)", (String) result.get(0).get("geom"));
    
    // associatedData
    JSONArray associatedDataArray = new JSONArray((String) result.get(0).get(Main.ASSOCIATED_DATA_PROPERTY));
    assertEquals(1, associatedDataArray.length());
    assertEquals(306787829, associatedDataArray.getJSONObject(0).getLong("osm_id"));
    assertNotNull(associatedDataArray.getJSONObject(0).getString("date_added"));
    assertEquals("park", associatedDataArray.getJSONObject(0).getString("type"));
    assertEquals("contains", associatedDataArray.getJSONObject(0).getString("relation"));

    // relationshipData - osm properties
    JSONObject relationshipData = new JSONObject((String) result.get(0).get("relationshipData"));
    assertEquals(306786518, relationshipData.getLong("osm_id"));
    assertNotNull(relationshipData.get("geom"));
    assertEquals("Coral Road", relationshipData.getString("name"));
  }

  @Test
  public void testGeoServerCreateWorkspace() throws Exception {
    int testCallIndex = 0; // create workspace is first call made to mock

    assertEquals("http://localhost:8080/geoserver/rest/workspaces", urlCaptor.getAllValues().get(testCallIndex));
    assertEquals("Basic Z2Vvc2VydmVyOmdlb3NlcnZlcg==", authHeaderCaptor.getAllValues().get(testCallIndex));
    assertEquals(MediaType.APPLICATION_XML, contentTypeCaptor.getAllValues().get(testCallIndex));

    // confirm post body - convert xml to json
    JSONObject postBody = XML.toJSONObject(postBodyCaptor.getAllValues().get(testCallIndex));
    assertEquals("sfpotrero", postBody.getJSONObject("workspace").getString("name"));
  }

  @Test
  public void testGeoServerCreatePostGisStore() throws Exception {
    int testCallIndex = 1; // create postgis store is second call made to mock

    assertEquals("http://localhost:8080/geoserver/rest/workspaces/sfpotrero/datastores", urlCaptor.getAllValues().get(testCallIndex));
    assertEquals("Basic Z2Vvc2VydmVyOmdlb3NlcnZlcg==", authHeaderCaptor.getAllValues().get(testCallIndex));
    assertEquals(MediaType.APPLICATION_XML, contentTypeCaptor.getAllValues().get(testCallIndex));

    // confirm post body - convert xml to json
    JSONObject postBody = XML.toJSONObject(postBodyCaptor.getAllValues().get(testCallIndex));
    assertEquals("sfpotrero", postBody.getJSONObject("dataStore").getString("name"));
    JSONObject connectionParameters =  postBody.getJSONObject("dataStore").getJSONObject("connectionParameters");
    assertEquals("test_sfpotrero", connectionParameters.getString("schema"));
    assertEquals("osm", connectionParameters.getString("database"));
    assertEquals(postgresqlContainer.getMappedPort(5432), connectionParameters.getInt("port"));
    assertEquals("osm", connectionParameters.getString("passwd"));
    assertEquals(postgresqlContainer.getHost(), connectionParameters.getString("host"));
    assertEquals("postgis", connectionParameters.getString("dbtype"));
    assertEquals("osm", connectionParameters.getString("user"));
  }

  @Test
  public void testGeoServerCreateFeatureType() throws Exception {
    int testCallIndex = 2; // create feature type is third call made to mock

    assertEquals("http://localhost:8080/geoserver/rest/workspaces/sfpotrero/datastores/sfpotrero/featuretypes", urlCaptor.getAllValues().get(testCallIndex));
    assertEquals("Basic Z2Vvc2VydmVyOmdlb3NlcnZlcg==", authHeaderCaptor.getAllValues().get(testCallIndex));
    assertEquals(MediaType.APPLICATION_XML, contentTypeCaptor.getAllValues().get(testCallIndex));

    // confirm post body - convert xml to json
    JSONObject postBody = XML.toJSONObject(postBodyCaptor.getAllValues().get(testCallIndex));
    JSONObject featureType = postBody.getJSONObject("featureType");
    assertEquals("sfpotrero_vis_osm_landusages", featureType.getString("name"));
    assertEquals("EPSG:3857", featureType.getString("srs"));
    assertEquals("REPROJECT_TO_DECLARED", featureType.getString("projectionPolicy"));
    assertEquals("EPSG:4326", featureType.getString("nativeCRS"));
    assertEquals(true, featureType.getBoolean("enabled"));
  }

  // TODO: move to shared library
  private ArrayList<Map<String, Object>> executePostgisQuery(String selectSQL) throws SQLException {
    Connection conn = DriverManager.getConnection(postgresqlContainer.getJdbcUrl(), "osm", "osm");
    Statement statement = conn.createStatement();
    ResultSet rs = statement.executeQuery(selectSQL);
      
    ResultSetMetaData md = rs.getMetaData();
    int columns = md.getColumnCount();
    ArrayList<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
    while (rs.next()) {
      HashMap<String, Object> row = new HashMap<String, Object>(columns);
      for(int i=1; i<=columns; ++i){           
        row.put(md.getColumnName(i),rs.getObject(i));
      }
      list.add(row);
    }

    statement.close();
    rs.close();

    return list;
  }

}

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.PostgisContainerProvider;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import me.callsen.taylor.osm2graph_geoserver.Config;
import me.callsen.taylor.osm2graph_geoserver.data.GraphDb;

@Testcontainers
@TestInstance(Lifecycle.PER_CLASS)
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

  private GraphDb db;

  @BeforeAll
  public void initResources() throws Exception {
    ClassLoader classLoader = getClass().getClassLoader();

    // copy graph db from resource into temporary (to avoid persisting changes)
    FileUtils.copyDirectory(new File(classLoader.getResource("neo4j/graph.db").getFile()), tempDirectory.toFile());

    // mock config object
    String testConfigString = Files.readString(Paths.get(classLoader.getResource("json/test-config.json").getFile()));
    JSONObject configObject = new JSONObject(testConfigString);

    // overlay postgis details from testcontainer
    JSONObject postgisConfig = new JSONObject();
    postgisConfig.put("host", postgresqlContainer.getHost());
    postgisConfig.put("port", postgresqlContainer.getMappedPort(5432)); // https://stackoverflow.com/questions/58150827/cant-connect-to-a-testcontainer-postgres-instance
    postgisConfig.put("database", "osm"); // testcontainers loads all data into the test database..
    postgisConfig.put("user", "osm");
    postgisConfig.put("password", "osm");
    postgisConfig.put("schema", "adjacentroads_sfpotrero");
    configObject.put("postgis", postgisConfig);

    // set graphdb temp location into config
    configObject.put("graphDbLocation", tempDirectory.toFile().getAbsolutePath());

    appConfig = new Config(configObject);

    // create stores
    db = new GraphDb(appConfig.getString("graphDbLocation"));
  }

  @Test
  public void testHelloWorld() throws Exception {
    System.out.println("Hello World");
  }

}

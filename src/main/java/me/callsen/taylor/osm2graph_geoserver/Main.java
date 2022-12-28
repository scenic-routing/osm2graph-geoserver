package me.callsen.taylor.osm2graph_geoserver;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.json.JSONObject;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import me.callsen.taylor.osm2graph_geoserver.data.GeoServerRestApi;
import me.callsen.taylor.osm2graph_geoserver.data.GraphDb;
import me.callsen.taylor.osm2graph_geoserver.data.PostgisDb;

public class Main {

  //shared static objects
  private static Config appConfig;

  // keep track of which visualization tables have been created (prevent extra calls to DB)
  private static Set<String> visDataTablesCreated = new HashSet<>();

  public static final int GRAPH_PAGINATION_AMOUNT = 5000;

  public static final String ASSOCIATED_DATA_PROPERTY = "associatedData";

  public static void main( String[] args ) throws Exception {

    //instantiate Config variables - must do in main because of exception handling
    appConfig = new Config();

    Class.forName("org.postgresql.Driver");
    String postgisUrl = "jdbc:postgresql://" + appConfig.getDbConfig().getString("host") + ":" + appConfig.getDbConfig().getString("port") + "/" + appConfig.getDbConfig().getString("database");

    String graphDbPath = appConfig.getString("graphDbLocation");

    System.out.println("OSM To Geoserver (via Postgis) initialized with following parameters: ");
    System.out.println("   graphDb: " + graphDbPath);
    System.out.println("   postgis: " + postgisUrl);
    System.out.println(" geoserver: " + GeoServerRestApi.getBaseUrl(appConfig.getGeoServerConfig()));

    // initialize PostGis
    PostgisDb postgisDb = new PostgisDb( appConfig, postgisUrl );

    // initialize GraphDB wrapper - facilitates loading of data into Neo4j Graph
    GraphDb graphDb = new GraphDb(graphDbPath);

    // initialize GeoServer wrapper
    GeoServerRestApi.initializeWorkspace(appConfig); // create workspace and store

    // retrieve number of relationships
    long wayCount = graphDb.getRelationshipCount();

    for (int pageNumber = 0; pageNumber * GRAPH_PAGINATION_AMOUNT < wayCount; ++pageNumber) {
      System.out.println(String.format("Processing page %s, up to relationship %s", pageNumber, (pageNumber + 1) * GRAPH_PAGINATION_AMOUNT));
      processRelationshipPage(graphDb, postgisDb, pageNumber);
    }

    // create featureType / layer in GeoServer
    //  - this is done after PostGIS data as been loaded so accurate bounding boxes can be computed
    JSONObject geoServerConfig = appConfig.getGeoServerConfig();
    String geoserverBaseUrl = GeoServerRestApi.getBaseUrl(geoServerConfig);
    String geoserverAuthHeader = GeoServerRestApi.getAuthHeader(geoServerConfig);
    for (String associatedDataPropertyTableName : visDataTablesCreated) {
      System.out.println("creating geoserver featureType '" + associatedDataPropertyTableName + "'");
      GeoServerRestApi.createFeatureType(geoserverBaseUrl, geoserverAuthHeader, geoServerConfig.getString("workspaceName"), geoServerConfig.getString("storeName"), associatedDataPropertyTableName);
    }

    // Close database connections
    postgisDb.close();
    graphDb.shutdown();

    System.out.println("Task complete");

  }

  public static void processRelationshipPage(GraphDb graphDb, PostgisDb postgisDb, int pageNumber) {
    
    Transaction tx = graphDb.getSharedTransaction();
    Result result = graphDb.getRelationshipPage(tx, pageNumber);
    
    String osmId = appConfig.getString("osmId");

    // loop through relationships returned in page
    while ( result.hasNext() ) {
      Map<String, Object> row = result.next();
      Relationship relationship = (Relationship)row.get("way");

      // loop through property names in associatedData - ensure uniqueness of property names
      Set<String> associatedDataPropertySet = new HashSet<>(Arrays.asList((String[]) relationship.getProperty(ASSOCIATED_DATA_PROPERTY)));

      for (String associatedDataProperty : associatedDataPropertySet) {
        
        String associatedDataPropertyTableName = String.format("%s_vis_%s", osmId, associatedDataProperty);

        // create vis table in PostGIS if it has not already been created
        if (!visDataTablesCreated.contains(associatedDataPropertyTableName)) {
          postgisDb.createVisualizationTable(associatedDataProperty, associatedDataPropertyTableName);
          visDataTablesCreated.add(associatedDataPropertyTableName);
        }

        // write relationship to Postgis visualization table
        postgisDb.writeVisualizationTableRow(associatedDataPropertyTableName, associatedDataProperty, relationship);
      }

    }

    graphDb.closeSharedTransaction();
  }

}
package me.callsen.taylor.osm2graph_geoserver;

import me.callsen.taylor.osm2graph_geoserver.data.GraphDb;
import me.callsen.taylor.osm2graph_geoserver.data.PostgisDb;

public class Main {

  //shared static objects
  public static Config appConfig;

  public static final int GRAPH_PAGINATION_AMOUNT = 5000;

  public static final String ASSOCIATED_DATA_PROPERTY = "associatedData";

  public static void main( String[] args ) throws Exception {

    //instantiate Config variables - must do in main because of exception handling
    appConfig = new Config();

    //initialize engine (powered by Postgis connection)
    Class.forName("org.postgresql.Driver");
    String postgisUrl = "jdbc:postgresql://" + appConfig.getDbConfig().getString("host") + ":" + appConfig.getDbConfig().getString("port") + "/" + appConfig.getDbConfig().getString("database");

    // initialize PostGis
    PostgisDb postgisDb = new PostgisDb( appConfig, postgisUrl );

    // Initialize GraphDB wrapper - facilitates loading of data into Neo4j Graph
    String graphDbPath = appConfig.getString("graphDbLocation");
    GraphDb graphDb = new GraphDb(graphDbPath);

    System.out.println("OSM To Geoserver (via Postgis) Initialized with following parameters: ");
    System.out.println("   graphDb: " + graphDbPath);
    System.out.println("   postgis: " + postgisUrl);

    // retrieve number of relationships - single relationship is returned per pair_id since visually cannot
    //  differentiate between 2 stacked relationships/ways
    //  TODO: this logic will need to be revisited for bi-directional support
    long wayCount = graphDb.getUniqueRelationshipCount();

    for (int pageNumber = 0; pageNumber * GRAPH_PAGINATION_AMOUNT < wayCount; ++pageNumber) {
      System.out.println(String.format("Processing page %s, up to relationship %s", pageNumber, (pageNumber + 1) * GRAPH_PAGINATION_AMOUNT));
      graphDb.processRelationshipPage(postgisDb, pageNumber);
    }

    // Close database connections
    postgisDb.close();
    graphDb.shutdown();

    System.out.println("Task complete");

  }

}
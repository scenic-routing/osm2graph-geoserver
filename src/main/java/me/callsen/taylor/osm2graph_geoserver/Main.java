package me.callsen.taylor.osm2graph_geoserver;

import me.callsen.taylor.osm2graph_geoserver.data.GraphDb;
import me.callsen.taylor.osm2graph_geoserver.data.PostgisDb;

public class Main {

  //shared static objects
  public static Config appConfig;

  public static void main( String[] args ) throws Exception {

    //instantiate Config variables - must do in main because of exception handling
    appConfig = new Config();

    //initialize engine (powered by Postgis connection)
    Class.forName("org.postgresql.Driver");
    String postgisUrl = "jdbc:postgresql://" + appConfig.getDbConfig().getString("host") + ":" + appConfig.getDbConfig().getString("port") + "/" + appConfig.getDbConfig().getString("database");

    //initialize Road Source Walker - used to discover adjacent road segments (Postgis)
    PostgisDb postgisStore = new PostgisDb( appConfig, postgisUrl );

    // Initialize GraphDB wrapper - facilitates loading of data into Neo4j Graph
    String graphDbPath = appConfig.getString("graphDbLocation");
    GraphDb graphDb = new GraphDb(graphDbPath);

    System.out.println("OSM To Geoserver (via Postgis) Initialized with following parameters: ");
    System.out.println("   graphDb: " + graphDbPath);
    System.out.println("   postgis: " + postgisUrl);

    // Close database connections
    postgisStore.close();
    graphDb.shutdown();

    System.out.println("Task complete");

  }

}
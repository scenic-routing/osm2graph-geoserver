package me.callsen.taylor.osm2graph_geoserver.data;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.nio.file.Paths;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

public class GraphDb {

  public GraphDatabaseService db;
  private DatabaseManagementService managementService;
  private Transaction sharedTransaction;


  public GraphDb(String graphDbPath) throws Exception {
    
    // initialize graph db connection
    managementService = new DatabaseManagementServiceBuilder( Paths.get( graphDbPath ) ).build();
    db = managementService.database( DEFAULT_DATABASE_NAME );
    // db = new GraphDatabaseFactory().newEmbeddedDatabase( new File( graphDbPath ) );
    System.out.println("Graph DB @ " + graphDbPath + " initialized");

  }

  public void shutdown(){
    
    // close db connection
    this.managementService.shutdown();
    
    System.out.println("Graph DB shutdown");

  }

}
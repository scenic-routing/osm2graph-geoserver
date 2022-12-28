package me.callsen.taylor.osm2graph_geoserver.data;

import static me.callsen.taylor.osm2graph_geoserver.Main.GRAPH_PAGINATION_AMOUNT;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.nio.file.Paths;
import java.util.Map;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
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

  public Transaction getSharedTransaction() {
    if (this.sharedTransaction != null) {
      return sharedTransaction;
    } else {
      this.sharedTransaction = this.db.beginTx();
      return this.sharedTransaction;
    }
  }

  public void closeSharedTransaction() {
    if (this.sharedTransaction != null) {
      this.sharedTransaction.close();
      this.sharedTransaction = null;
    }
  }

  public long getRelationshipCount() {

    long count = 0;

    Transaction tx = this.db.beginTx();
    try ( Result result = tx.execute( "MATCH ()-[r]-() WHERE NOT isEmpty(r.associatedData) RETURN COUNT(r) AS total" ) ) {
      while ( result.hasNext() ) {
        Map<String, Object> row = result.next();
        count = (Long) row.get("total");
      }
    } finally {
      tx.close();  
    }

    return count;
  }

  public Result getRelationshipPage(Transaction tx, int pageNumber) {
    long startIndex = pageNumber * GRAPH_PAGINATION_AMOUNT;
    Result result = tx.execute( String.format("MATCH ()-[r]-() WHERE NOT isEmpty(r.associatedData) RETURN r as way ORDER BY r.osm_id DESC SKIP %s LIMIT %s", startIndex, GRAPH_PAGINATION_AMOUNT ) );
    return result;
  }

  public void shutdown(){
    
    // close db connection
    this.managementService.shutdown();
    
    System.out.println("Graph DB shutdown");

  }

}
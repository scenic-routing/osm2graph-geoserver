package me.callsen.taylor.osm2graph_geoserver.data;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;

import static me.callsen.taylor.osm2graph_geoserver.Main.GRAPH_PAGINATION_AMOUNT;
import static me.callsen.taylor.osm2graph_geoserver.Main.ASSOCIATED_DATA_PROPERTY;

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

  public long getUniqueRelationshipCount() {

    long count = 0;

    Transaction tx = this.db.beginTx();
    try ( Result result = tx.execute( "MATCH ()-[r]-() WHERE NOT isEmpty(r.associatedData) RETURN COUNT(DISTINCT r.pair_id) AS total" ) ) {
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
    Result result = tx.execute( String.format("MATCH ()-[r]-() WHERE NOT isEmpty(r.associatedData) RETURN DISTINCT r.pair_id as pair_id, last(collect(r)) as way SKIP %s LIMIT %s", startIndex, GRAPH_PAGINATION_AMOUNT ) );
    return result;
  }

  public void shutdown(){
    
    // close db connection
    this.managementService.shutdown();
    
    System.out.println("Graph DB shutdown");

  }

}
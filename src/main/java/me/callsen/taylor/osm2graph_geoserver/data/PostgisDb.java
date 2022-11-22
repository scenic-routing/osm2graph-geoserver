package me.callsen.taylor.osm2graph_geoserver.data;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import me.callsen.taylor.osm2graph_geoserver.Config;

public class PostgisDb {


  private String postgisSchema;

  private Connection conn;
  
  public PostgisDb( Config appConfig, String postgisUrl ) throws Exception {
    
    //initialize connection to Postgis Shape Source
    this.postgisSchema = appConfig.getDbConfig().getString("schema");

    conn = DriverManager.getConnection(postgisUrl,appConfig.getDbConfig().getString("user"), appConfig.getDbConfig().getString("password"));
    System.out.println( "PostgisDb initialized and will persist through app completion" );
  }
  
  public String executeQuery( String sql, int columnNum ) {
    
    String returnString = null;
    
    try {
    
      Statement st = this.conn.createStatement();
      ResultSet rs = st.executeQuery(sql);
      if (!rs.next()) return "";
      returnString = rs.getString(columnNum);
      rs.close();
      st.close();
      
    } catch(Exception e) { e.printStackTrace(); }
    
    return returnString;
    
  }
  
  public int executeUpdate( String sql ) {
    
    int responseInt = -1;
    
    try {
    
      Statement st = this.conn.createStatement();
      responseInt = st.executeUpdate(sql);
      
    } catch(Exception e) { System.out.println( "ERROR executing Update Query:" + sql ); /* System.out.println( responseInt ); */ }
    
    return responseInt;

  }

  public void close() throws SQLException {
    System.out.println("PostgisDb shutdown");
    this.conn.close();
  }
  
}

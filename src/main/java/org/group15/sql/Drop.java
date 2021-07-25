package org.group15.sql;

import org.group15.database.Table;

import java.io.FileWriter;

public class Drop {

  FileWriter eventLogsWriter;

  Table table;

  public Drop(FileWriter eventLogsWriter) {
    this.eventLogsWriter = eventLogsWriter;
    table = new Table(eventLogsWriter);
  }

  public boolean parseDropTableStatement(String query, String schemaName) throws Exception {
    String[] queryParts = query.split("\\s+");

    if (queryParts.length < 3) {
      this.eventLogsWriter.append("Syntax error: Error while parsing drop " +
              "table query").append("\n");
      this.eventLogsWriter.close();
      throw new Exception("Syntax error: Error while parsing drop table query");
    }

    if (!queryParts[1].equalsIgnoreCase("TABLE")) {
      this.eventLogsWriter.append("Syntax error: TABLE keyword not found " +
              "in drop table query").append("\n");
      this.eventLogsWriter.close();
      throw new Exception("Syntax error: TABLE keyword not found in " +
              "drop table query");
    }
    if(queryParts.length == 3){
      String tableName = queryParts[2];

      return table.dropTable(schemaName, tableName);
    }
    else{
      for(int i =2; i < queryParts.length; i++){
        String tableName = queryParts[i];
        tableName = tableName.replaceAll(",$", "");
        table.dropTable(schemaName, tableName);
      }
    }
    return true;
  }
}

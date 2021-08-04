package org.group15.sql;

import org.group15.database.Table;

import java.io.FileWriter;

public class Drop {

  FileWriter eventLogsWriter;

  Table table;

  boolean isTransaction;

  boolean isBulkOperation;

  public Drop(FileWriter eventLogsWriter, boolean isTransaction, boolean isBulkOperation) {
    this.eventLogsWriter = eventLogsWriter;
    this.isTransaction = isTransaction;
    this.isBulkOperation = isBulkOperation;
    table = new Table(eventLogsWriter);
  }

  public boolean parseDropTableStatement(String query, String schemaName) throws Exception {
    String[] queryParts = query.split("\\s+");

    if (queryParts.length != 3) {
      this.eventLogsWriter.append("Syntax error: Error while parsing drop " + "table query").append("\n");
      throw new Exception("Syntax error: Error while parsing drop table query");
    }

    if (!queryParts[1].equalsIgnoreCase("TABLE")) {
      this.eventLogsWriter.append("Syntax error: TABLE keyword not found " + "in drop table query").append("\n");
      throw new Exception("Syntax error: TABLE keyword not found in " + "drop table query");
    }

    String tableName = queryParts[2];

    if (!isTransaction) {
      return table.dropTable(schemaName, tableName, isBulkOperation);
    }
    return true;
  }

}

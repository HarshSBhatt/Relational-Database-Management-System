package org.group15.sql;

import org.group15.io.SchemaIO;
import org.group15.io.TableIO;

import java.io.FileWriter;
import java.io.IOException;

public class Select {

  SchemaIO schemaIO;

  TableIO tableIO;

  FileWriter eventLogsWriter;

  public Select(FileWriter eventLogsWriter) {
    this.schemaIO = new SchemaIO();
    this.tableIO = new TableIO();
    this.eventLogsWriter = eventLogsWriter;
  }

  public boolean parseUseSchemaStatement(int size, String[] queryParts) throws IOException {
    if (size == 2) {
      String schemaName = queryParts[1].toLowerCase();
      if (schemaIO.isExist(schemaName)) {
        System.out.println(schemaName + " selected");
        this.eventLogsWriter.append("Schema: ").append(schemaName).append(" " +
            "selected").append("\n");
        return true;
      } else {
        this.eventLogsWriter.append("Schema: ").append(schemaName).append(" does " +
            "not exist").append("\n");
        System.out.println("Schema: " + schemaName + " does not exist");
      }
    } else {
      this.eventLogsWriter.append("Syntax error: Please check your syntax for USE Schema").append("\n");
      System.out.println("Syntax error: Please check your syntax for USE Schema");
    }
    return false;
  }

}

package org.group15.sql;

import org.group15.io.SchemaIO;
import org.group15.io.TableIO;

import java.io.FileWriter;
import java.io.IOException;

public class Show {

  SchemaIO schemaIO;

  TableIO tableIO;

  FileWriter eventLogsWriter;

  public Show(FileWriter eventLogsWriter) {
    this.schemaIO = new SchemaIO();
    this.tableIO = new TableIO();
    this.eventLogsWriter = eventLogsWriter;
  }

  public void parseShowSchemaStatement(int size, String[] queryParts) throws IOException {
    if (size == 2 && queryParts[1].equalsIgnoreCase("DATABASES")) {
      schemaIO.list();
      this.eventLogsWriter.append("Schema listed successfully").append("\n");
    } else {
      this.eventLogsWriter.append("Syntax error: Please check your syntax for SHOW Schema").append("\n");
      System.out.println("Syntax error: Please check your syntax for SHOW Schema");
    }
  }

}

package org.group15.sql;

import org.group15.io.SchemaIO;
import org.group15.io.TableIO;

public class Show {

  SchemaIO schemaIO;

  TableIO tableIO;


  public Show() {
    this.schemaIO = new SchemaIO();
    this.tableIO = new TableIO();
  }

  public void parseShowSchemaStatement(int size, String[] queryParts) {
    if (size == 2 && queryParts[1].equalsIgnoreCase("DATABASES")) {
      schemaIO.list();
    } else {
      System.out.println("Syntax error: Please check your syntax for SHOW Schema");
    }
  }

}

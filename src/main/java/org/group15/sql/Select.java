package org.group15.sql;

import org.group15.io.SchemaIO;
import org.group15.io.TableIO;

public class Select {

  SchemaIO schemaIO;

  TableIO tableIO;


  public Select() {
    this.schemaIO = new SchemaIO();
    this.tableIO = new TableIO();
  }

  public boolean parseUseSchemaStatement(int size, String[] queryParts) {
    if (size == 2) {
      String schemaName = queryParts[1].toLowerCase();
      if (schemaIO.isExist(schemaName)) {
        System.out.println(schemaName + " selected");
        return true;
      } else {
        System.out.println("Schema: " + schemaName + " does not exist");
      }
    } else {
      System.out.println("Syntax error: Please check your syntax for USE Schema");
    }
    return false;
  }

}

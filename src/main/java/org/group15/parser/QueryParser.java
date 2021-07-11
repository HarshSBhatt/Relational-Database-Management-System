package org.group15.parser;

import org.group15.database.Schema;

public class QueryParser {

  Schema schema = new Schema();

  SchemaParser schemaParser = new SchemaParser();

  public void parse(String query) {
    String[] queryParts = query.split(" ");
    String dbOperation = queryParts[0];

    int size;
    String schemaName;
    boolean isValidSyntax;

    switch (dbOperation.toUpperCase()) {
      case "CREATE":
        size = queryParts.length;
        isValidSyntax = schemaParser.parseCreateSchemaStatement(size, queryParts);
        if (isValidSyntax) {
          schemaName = queryParts[2].toLowerCase();
          schema.setSchemaName(schemaName);
        }
        break;
      case "USE":
        size = queryParts.length;
        isValidSyntax = schemaParser.parseUseSchemaStatement(size, queryParts);
        if (isValidSyntax) {
          schemaName = queryParts[1].toLowerCase();
          schema.setSchemaName(schemaName);
        }
        break;
      case "SHOW":
        size = queryParts.length;
        schemaParser.parseShowSchemaStatement(size, queryParts);
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + dbOperation);
      case "CREATE_TABLE":
        if (schema.getSchemaName() == null) {
          System.out.println("Error! Schema is not selected");
        } else {
          // Logic if schema is selected
        }
    }
  }

}

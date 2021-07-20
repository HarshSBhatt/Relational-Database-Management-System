package org.group15.parser;

import org.group15.database.Schema;
import org.group15.database.Table;
import org.group15.sql.Create;
import org.group15.sql.Select;
import org.group15.sql.Show;

public class QueryParser {

  Schema schema = new Schema();

  Table table = new Table();

  Create createSQL = new Create();

  Select selectSQL = new Select();

  Show showSQL = new Show();


  public void parse(String query) {
    String[] queryParts = query.split(" ");
    String dbOperation = queryParts[0];

    int size;
    String schemaName;
    boolean isValidSyntax;
    String tableName;

    switch (dbOperation.toUpperCase()) {
      case "CREATE":
        size = queryParts.length;
        isValidSyntax = createSQL.parseCreateSchemaStatement(size,
            queryParts);
        if (isValidSyntax) {
          schemaName = queryParts[2].toLowerCase();
          schema.setSchemaName(schemaName);
        }
        break;
      case "USE":
        size = queryParts.length;
        isValidSyntax = selectSQL.parseUseSchemaStatement(size, queryParts);
        if (isValidSyntax) {
          schemaName = queryParts[1].toLowerCase();
          schema.setSchemaName(schemaName);
        }
        break;
      case "SHOW":
        size = queryParts.length;
        showSQL.parseShowSchemaStatement(size, queryParts);
        break;

      case "CREATE_TABLE":
        size = queryParts.length;
        String selectedSchema = schema.getSchemaName();
        //System.out.println(size);
        if (schema.getSchemaName() == null) {
          System.out.println("Error! Schema is not selected");
        } else {
          isValidSyntax = createSQL.parseCreateTableStatement(size,
              queryParts,
              selectedSchema);
          if (isValidSyntax) {
            tableName = queryParts[1].toLowerCase();
            table.setTableName(tableName);
          }
        }
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + dbOperation);
    }
  }

}

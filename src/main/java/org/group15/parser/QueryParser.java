package org.group15.parser;

import org.group15.database.Schema;

public class QueryParser {
    Schema schema = new Schema();
    SchemaParser schemaParser = new SchemaParser();

    public void parse(String query) {
        String[] queryParts = query.split(" ");
        String dbOperation = queryParts[0].toUpperCase();
        int size;

        switch (dbOperation) {
            case "CREATE":
                size = queryParts.length;
                if (size == 3 && queryParts[1].equalsIgnoreCase("SCHEMA")) {
                    String schemaName = queryParts[2].toLowerCase();
                    boolean isValidSyntax = schemaParser.parseCreateSchemaStatement(schemaName);
                    if (isValidSyntax) {
                        schema.setSchemaName(schemaName);
                    }
                } else {
                    System.out.println("Syntax error: Please check your syntax for Create Schema");
                }
                break;
            case "USE":
                size = queryParts.length;
                if (size == 2) {
                    String schemaName = queryParts[1].toLowerCase();
                    boolean isValidSyntax = schemaParser.parseUseSchemaStatement(schemaName);
                    if (isValidSyntax) {
                        System.out.println("here");
                        schema.setSchemaName(schemaName);
                    }
                } else {
                    System.out.println("Syntax error: Please check your syntax for USE Schema");
                }
                break;
            case "SHOW":
                size = queryParts.length;
                if (size == 2 && queryParts[1].equalsIgnoreCase("DATABASES")) {
                    schema.list();
                } else {
                    System.out.println("Syntax error: Please check your syntax for SHOW Schema");
                }
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + dbOperation);
        }
    }
}

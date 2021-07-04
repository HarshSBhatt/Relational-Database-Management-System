package org.group15.parser;

import org.group15.io.SchemaIO;

public class SchemaParser {

    SchemaIO schemaIO;

    public SchemaParser() {
        this.schemaIO = new SchemaIO();
    }

    // USE schema
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

    public boolean parseCreateSchemaStatement(int size, String[] queryParts) {
        boolean isValidSyntax = false;
        if (size == 3 && queryParts[1].equalsIgnoreCase("SCHEMA")) {
            String schemaName = queryParts[2].toLowerCase();
            isValidSyntax = schemaIO.create(schemaName);
        } else {
            System.out.println("Syntax error: Please check your syntax for Create Schema");
        }
        return isValidSyntax;
    }

    public void parseShowSchemaStatement(int size, String[] queryParts) {
        if (size == 2 && queryParts[1].equalsIgnoreCase("DATABASES")) {
            schemaIO.list();
        } else {
            System.out.println("Syntax error: Please check your syntax for SHOW Schema");
        }
    }
}

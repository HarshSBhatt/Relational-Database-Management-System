package org.group15.parser;

import org.group15.io.SchemaIO;

public class SchemaParser {

    SchemaIO schemaIO;

    public SchemaParser() {
        this.schemaIO = new SchemaIO();
    }

    // USE schema
    public boolean parseUseSchemaStatement(String schemaName) {
        if (schemaIO.isExist(schemaName)) {
            System.out.println(schemaName + " selected");
            return true;
        } else {
            System.out.println("Schema does not exist");
            return false;
        }
    }

    public boolean parseCreateSchemaStatement(String schemaName) {
        return schemaIO.create(schemaName);
    }
}

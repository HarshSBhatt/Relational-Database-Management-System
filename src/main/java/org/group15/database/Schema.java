package org.group15.database;

import org.group15.io.SchemaIO;

public class Schema {

    SchemaIO schemaIO = new SchemaIO();

    private String schemaName;

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public void create(String schemaName) {
        schemaIO.create(schemaName);
    }

    public void isExist(String schemaName) {
        if (schemaIO.isExist(schemaName)) {
            System.out.println("Schema exists");
        } else {
            System.out.println("Schema does not exist");
        }
    }

    public void list() {
        schemaIO.list();
    }
}

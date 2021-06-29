package org.group15.database;

import org.group15.io.SchemaIO;

import java.io.File;

public class Schema {

    SchemaIO schemaIO = new SchemaIO();

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

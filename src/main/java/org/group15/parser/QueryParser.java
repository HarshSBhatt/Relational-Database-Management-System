package org.group15.parser;

import org.group15.database.Schema;

public class QueryParser {
    Schema schema = new Schema();

    public void parse(String query) {
        String[] list = query.split(" ");
        String dbOperation = list[0].toUpperCase();
        String schemaName = list[2].toLowerCase();

        switch (dbOperation) {
            case "CREATE":
                schema.create(schemaName);
                break;
            case "USE":
                System.out.println("USE");
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + dbOperation);
        }
    }
}

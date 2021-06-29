package org.group15;

import org.group15.database.Schema;
import org.group15.parser.QueryParser;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class App {
    public static void main(String[] args) {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        Schema schema = new Schema();
        QueryParser queryParser = new QueryParser();

        boolean valid = true;
        while (valid) {
            System.out.println("--------------------------");
            System.out.println("PRESS 1: List Schemas");
            System.out.println("PRESS 2: Write Query");
            System.out.println("PRESS 3: Generate Logs");
            System.out.println("PRESS 4: Generate ERD");
            System.out.println("PRESS 5: Exit");
            System.out.println("--------------------------");
            try {
                System.out.print("Select any input: ");
                int input = Integer.parseInt(br.readLine());
                switch (input) {
                    case 1:
                        schema.list();
                        break;
                    case 2:
                        System.out.print("Query: ");
                        String query = br.readLine();
                        queryParser.parse(query);
                        break;
                    case 3:
                        System.out.println("Logs");
                        break;
                    case 4:
                        System.out.println("ERD");
                        break;
                    case 5:
                        valid = false;
                        break;
                    default:
                        break;
                }
            } catch (Exception e) {
                System.out.println("Invalid Input!");
                System.out.println(e.getMessage());
            }
        }
    }
}

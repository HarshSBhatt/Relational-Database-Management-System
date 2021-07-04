package org.group15;

import org.group15.parser.QueryParser;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class App {
    public static void main(String[] args) {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        QueryParser queryParser = new QueryParser();

        boolean valid = true;
        while (valid) {
            System.out.println("--------------------------");
            System.out.println("PRESS 1: Write Query");
            System.out.println("PRESS 2: Generate Logs");
            System.out.println("PRESS 3: Generate ERD");
            System.out.println("PRESS 4: Exit");
            System.out.println("--------------------------");
            try {
                System.out.print("Select any input: ");
                int input = Integer.parseInt(br.readLine());
                switch (input) {
                    case 1:
                        System.out.print("Query: ");
                        String query = br.readLine();
                        queryParser.parse(query);
                        break;
                    case 2:
                        System.out.println("Logs");
                        break;
                    case 3:
                        System.out.println("ERD");
                        break;
                    case 4:
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

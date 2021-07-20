package org.group15.core;

import org.group15.parser.QueryParser;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class Group15 {

  public void rdbmsProvider() {
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    QueryParser queryParser = new QueryParser();

    boolean valid = true;

    while (valid) {
      try {
        String input = br.readLine();
        if (input.equalsIgnoreCase("exit")) {
          valid = false;
        } else {
          queryParser.parse(input);
        }
      } catch (Exception e) {
        System.out.println("Invalid Input!");
        System.out.println(e.getMessage());
      }
    }
  }

}

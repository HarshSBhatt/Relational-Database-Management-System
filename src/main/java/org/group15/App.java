package org.group15;

import org.group15.auth.Auth;
import org.group15.core.Group15;

import java.util.Scanner;

public class App {

  public static void main(String[] args) {
    Auth auth = new Auth();
    Group15 group15 = new Group15();

    Scanner sc = new Scanner(System.in);

    System.out.print("Enter username: ");
    String username = sc.next();

    System.out.print("Enter password: ");
    String password = sc.next();

    boolean isAuthenticated = auth.login(username, password);
    if (!isAuthenticated) {
      System.out.print("Do you want to register? (Y/N) ");
      String inputChar = sc.next();
      if (inputChar.equalsIgnoreCase("y")) {
        boolean isRegistered = auth.register(username, password);
        if (isRegistered) {
          System.out.println("New User Created");
          // Main provider for query parsing
          group15.rdbmsProvider();
        }
      } else {
        System.out.println("User selected no. Process terminated");
      }
    } else {
      // Main provider for query parsing
      group15.rdbmsProvider();
    }
  }

}

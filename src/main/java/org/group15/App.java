package org.group15;

import org.group15.auth.Auth;
import org.group15.auth.User;
import org.group15.core.Group15;

import java.io.IOException;
import java.util.Scanner;

public class App {

  public static void main(String[] args) throws Exception {
    Auth auth = new Auth();
    User user = new User();
    /**
     * Delete this
     */
    String username1 = "abc";
    /**
     * Delete this
     */
    Group15 group15 = new Group15();
    /**
     *
     */
    group15.rdbmsProvider(username1);
    /**
     *
     */
//    Scanner sc = new Scanner(System.in);
//
//    System.out.print("Enter username: ");
//    String username = sc.next();
//
//    System.out.print("Enter password: ");
//    String password = sc.next();
//
//    boolean isAuthenticated = auth.login(username, password);
//    if (!isAuthenticated) {
//      System.out.print("Do you want to register? (Y/N) ");
//      String inputChar = sc.next();
//      if (inputChar.equalsIgnoreCase("y")) {
//        boolean isRegistered = auth.register(username, password);
//        if (isRegistered) {
//          System.out.println("New User Created");
//          user.setUsername(username);
//          // Main provider for query parsing
//          group15.rdbmsProvider(username);
//        }
//      } else {
//        System.out.println("User selected NO! Process terminated");
//      }
//    } else {
//      user.setUsername(username);
//      // Main provider for query parsing
//      group15.rdbmsProvider(username);
//    }
  }

}

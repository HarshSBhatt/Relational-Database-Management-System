package org.group15.auth;

import java.io.*;

public class Auth {

  public Boolean login(String username, String password) {
    boolean flag = false;

    try {
      File file = new File("sign-in.dp15");

      if (!file.exists()) {
        file.createNewFile();
      }

      FileReader fr = new FileReader(file);
      BufferedReader br = new BufferedReader(fr);

      String line = br.readLine();
      while (line != null) {
        if (username.equalsIgnoreCase(line)) {
          line = br.readLine();
          if (password.equalsIgnoreCase(line)) {
            flag = true;
            System.out.println("Authorization successful");
          } else {
            System.out.println("Wrong credentials!");
          }
        }
        line = br.readLine();
      }

      br.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return flag;
  }

  public Boolean register(String username, String password) {
    try {
      File file = new File("sign-in.dp15");
      FileReader fileReader = new FileReader(file);
      BufferedReader bufferedReader = new BufferedReader(fileReader);

      FileWriter fileWriter = new FileWriter(file, true);   //true means while is appended

      String line = bufferedReader.readLine();

      while (line != null) {
        if (username.equalsIgnoreCase(line)) {
          System.out.println("User with this username already exists!");
          return false;
        }
        line = bufferedReader.readLine();
      }

      fileWriter.append(username).append("\n").append(password).append("\n\n");
      fileWriter.close();
      bufferedReader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return true;
  }

}

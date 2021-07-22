package org.group15.auth;

public class Encryption {

  int shiftKey;

  public static final String ALPHABET = "abcdefghijklmnopqrstuvwxyz";

  public Encryption(int shiftKey) {
    this.shiftKey = shiftKey;
  }

  public String encode(String message) {
    message = message.toLowerCase();
    StringBuilder cipherText = new StringBuilder();
    for (int i = 0; i < message.length(); i++) {
      int charPosition = ALPHABET.indexOf(message.charAt(i));
      int keyVal = (shiftKey + charPosition) % 26;
      char replaceVal = ALPHABET.charAt(keyVal);
      cipherText.append(replaceVal);
    }
    return cipherText.toString();
  }

  public String decode(String message) {
    message = message.toLowerCase();
    StringBuilder plainText = new StringBuilder();
    for (int i = 0; i < message.length(); i++) {
      int charPosition = ALPHABET.indexOf(message.charAt(i));
      int keyVal = (charPosition - shiftKey) % 26;
      if (keyVal < 0) {
        keyVal = ALPHABET.length() + keyVal;
      }
      char replaceVal = ALPHABET.charAt(keyVal);
      plainText.append(replaceVal);
    }
    return plainText.toString();
  }

}

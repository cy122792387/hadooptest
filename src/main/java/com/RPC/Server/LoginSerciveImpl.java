package com.RPC.Server;

import com.RPC.protocol.LoginServiceInterface;

public class LoginSerciveImpl implements LoginServiceInterface {
  public String login(String username, String password) {
    if (password.equals("123")) {
      System.out.println(username + " logged in !");
      return username + " logged success ! ";
    }
    return "username or passwd ERROR !";
  }
}
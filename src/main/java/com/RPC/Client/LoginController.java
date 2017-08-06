package com.RPC.Client;

import com.RPC.protocol.LoginServiceInterface;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

public class LoginController {
  public static void main(String[] args) throws IOException {
    LoginServiceInterface proxy =
        RPC.getProxy(LoginServiceInterface.class,
            LoginServiceInterface.versionID,
            new InetSocketAddress("127.0.0.1", 10000),
            new Configuration());
    String res = proxy.login("fengjie", "123");
    System.out.println(res);
  }
}

package com.RPC.Server;

import com.RPC.protocol.LoginServiceInterface;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Builder;
import org.apache.hadoop.ipc.RPC.Server;

import java.io.IOException;

public class Starter {
  public static void main(String[] args) throws IOException {
    Builder builder = new RPC.Builder(new Configuration());
    builder.setBindAddress("localhost")
        .setPort(10000)
        .setProtocol(LoginServiceInterface.class)
        .setInstance(new LoginSerciveImpl());
    Server server = builder.build();
    System.out.println("service started ! ");
    server.start();
  }
}

package cjb.test.niopingpong;

import java.io.IOException;

public class PingPong {

  private static final int SIZE = 1024;
  private static final int COUNT = 50;

  /**
   * FC20 with jdk 1.8.0 on an i7-3930K:
   * Client: 3.957000 s, 12.635835 msgs/s, 202.173364 kbit/s.
   */
  public static void main(String[] args) throws IOException {

    // start the server
    PongServer server = new PongServer(0);
    new Thread(server, "Server").start();

    // start the client
    PingClient.pingServer("localhost", server.port(), SIZE, COUNT);

    System.exit(0);
  }
}

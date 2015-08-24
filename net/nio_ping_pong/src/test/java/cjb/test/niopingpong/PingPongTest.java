package cjb.test.niopingpong;

import java.io.IOException;
import org.junit.Test;

/**
 * Simple test validate ping-pong even works.
 */
public class PingPongTest {

  @Test
  public void test () throws IOException {

    try (PongServer server = new PongServer(0)) {
      new Thread(server, "Server").start();

      // start the client
      PingClient.pingServer("localhost", server.port(), 10, 2);
    }
  }
}

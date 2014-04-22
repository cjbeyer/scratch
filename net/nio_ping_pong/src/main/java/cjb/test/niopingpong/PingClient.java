package cjb.test.niopingpong;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

/**
 * Simple blocking client that Ping-Pongs with the PongServer.
 */
class PingClient {

  /**
   * @param hostname  the name hostname where PongServer is running.
   * @param port      the port that PongServer is accepting new connections.
   * @param xferSize  the size of the data to send as a message.
   * @param xferCount the number of times to send the message.
   */
  public static void pingServer(String hostname, int port, int xferSize, int xferCount) throws IOException {
    System.out.println("Client started");

    // build the transfer buffer
    byte[] xferBuffer = new byte[xferSize];
    for (int i = 0; i < xferSize; i++) {
      xferBuffer[i] = (byte) i;
    }

    // connect to the PongServer
    Socket socket = new Socket(hostname, port);
    DataOutputStream output = new DataOutputStream(socket.getOutputStream());
    DataInputStream input = new DataInputStream(socket.getInputStream());

    long start = System.currentTimeMillis();

    // send/receive the buffer xferCount times
    for (int i=0; i < xferCount; i++) {
      byte[] readBuf = new byte[xferSize];

      // write the message
      output.writeInt(xferSize);
      output.write(xferBuffer);

      // read the message
      int msgSize = input.readInt();
      int red = input.read(readBuf);

      // validate the message
      assert msgSize == xferSize;
      assert red == xferSize;
    }

    double secs = (System.currentTimeMillis() - start) / 1000.0;
    double msgsPerSec = xferCount / secs;
    double kbiPerSec = xferBuffer.length * 2 * 8 * xferCount / secs / 1024;
    String msg = String.format("Client: %f s, %f msgs/s, %f kbit/s.", secs, msgsPerSec, kbiPerSec);
    System.out.println(msg);

    output.close();
    input.close();
    socket.close();
  }
}

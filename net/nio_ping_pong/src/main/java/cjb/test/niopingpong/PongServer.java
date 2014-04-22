package cjb.test.niopingpong;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Simple non-blocking NIO Server that:
 * 1) Accepts new Connections
 * 2) Reads a int msg size and byte block of data
 * 3) Writes back an int msg size and the block of data
 */
class PongServer implements Runnable {

  /** Default size of the input buffers. */
  private static final int DEFAULT_INPUT_BUFFER_SIZE = 8192;

  /** The Selector that all sockets will use. */
  private final Selector selector;

  /** Lock to guard access to the Selector. */
  private final Lock selectorLock = new ReentrantLock();

  /** One read-buffer per connection. */
  private final Map<SocketChannel, ByteBuffer> readBuffers = new HashMap<>();

  /** Queue of output buffer to write. */
  private final Map<SocketChannel, Queue<ByteBuffer>> writeBuffers = new ConcurrentHashMap<>();

  /** Simple threadpool so we aren't constantly allocating new Threads. */
  private final Executor executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

  /** Instantiates the Pong server on the specified port. */
  PongServer(int port) throws IOException {

    // create the selector
    selector = SelectorProvider.provider().openSelector();

    // create the server sock and set it to non-blocking
    ServerSocketChannel serverChannel = ServerSocketChannel.open();
    serverChannel.configureBlocking(false);

    // bind the socket to the specified local port and register it to accept connections
    serverChannel.socket().bind(new InetSocketAddress(port));
    serverChannel.register(selector, SelectionKey.OP_ACCEPT);
  }

  /** Main I/O Loop. */
  public void run() {
    System.out.println("Server Started");

    while (true) {
      try {
        // make sure someone isn't trying to modify the selector
        selectorLock.lock();
        selectorLock.unlock();

        // block until we have an I/O Event
        selector.select();

        // parse all of the I/O events
        for (SelectionKey key : selector.selectedKeys()) {
          if (key.isValid()) {

            if (key.isAcceptable()) {
              accept(key);

            } else if (key.isReadable()) {
              read(key);

            } else if (key.isWritable()) {
              write(key);
            }
          }
        }

        selector.selectedKeys().clear();

      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /** Accepts a socket connection request. */
  private void accept(SelectionKey key) throws IOException {
    ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();

    SocketChannel socketChannel = serverSocketChannel.accept();
    socketChannel.configureBlocking(false);
    socketChannel.register(selector, SelectionKey.OP_READ);

    // initialize the I/O structures
    readBuffers.put(socketChannel, (ByteBuffer)ByteBuffer.allocate(DEFAULT_INPUT_BUFFER_SIZE).mark());
    writeBuffers.put(socketChannel, new LinkedTransferQueue<>());
  }

  /** Reads input messages (size + bytes) then queues the message to be send back to the client. */
  private void read(final SelectionKey key) throws IOException {
    final SocketChannel socketChannel = (SocketChannel) key.channel();

    ByteBuffer readBuffer = readBuffers.get(socketChannel);
    assert readBuffer.hasRemaining();

    int numRead = socketChannel.read(readBuffer);

    // see if the client close the connection
    if (numRead == -1) {
      key.cancel();
      return;
    }

    assert numRead > 0;

    // this is the end of the valid data in the buffer
    int endOfData = readBuffer.position();

    // move the position back to the beginning of the unprocessed message at mark()
    readBuffer.reset();

    // loop while we have messages in the input buffer
    while( processMessage(readBuffer, endOfData, key) );

    // if we have room in the buffer to read more data
    if (endOfData < readBuffer.limit()) {
      // move the position to the end of the last read input
      // mark is already at the beginning of the partial message
      readBuffer.position(endOfData);
    }
    // allocate a new input buffer
    else {
      int size = DEFAULT_INPUT_BUFFER_SIZE;

      // if we know the message size, make sure don't need to allocate a larger buffer
      if (readBuffer.remaining() >= 4) {
        int msgSize = readBuffer.getInt( readBuffer.position() ) + 4;
        if (msgSize > size) {
          size = msgSize;
        }
      }

      ByteBuffer newBuf = ByteBuffer.allocate(size);
      newBuf.mark();

      // copy over any data we've already read in
      if (readBuffer.position() != readBuffer.limit()) {
        newBuf.put(readBuffer);
      }

      readBuffers.put(socketChannel, newBuf);
    }
  }

  /**
   *  Process an input message.  Upon returning the ByteBuffer position will be at the beginning
   *  of the unprocessed partial message, or at the endOfData if there is no partial message.
   *
   * @param inputBuf The ByteBuffer that contains the read-in data - position should be at the msg size.
   * @param endOfData the end of the valid data read into the input buffer.
   * @param key The SelectionKey that holds the channel the data was read from.
   * @return true if there might be more messages to process in the input buffer
   */
  private boolean processMessage(ByteBuffer inputBuf, int endOfData, SelectionKey key) {

    // first make sure we have enough data to read the 4-byte message size
    if ((endOfData - inputBuf.position()) <= 4)  {
      return false;
    }

    // get the message size
    int msgSize = inputBuf.getInt();
    assert msgSize > 0;

    // if we haven't read the full message, just return false
    if ((endOfData - inputBuf.position()) < msgSize) {
      inputBuf.position(inputBuf.position() - 4);
      return false;
    }

    // re-wrap the data buffer to queue it for output
    ByteBuffer out = ByteBuffer.wrap(inputBuf.array(), inputBuf.position(), msgSize);

    // move the input buffer's mark forward to the beginning of the next message
    inputBuf.position( inputBuf.position() + msgSize );
    inputBuf.mark();

    // create a task to just resend the data back to the client
    executor.execute(() -> { addMessage(key, out); });

    return inputBuf.remaining() > 0;
  }

  /** Writes the queued output ByteBuffers to the socket. */
  private void write(SelectionKey key) throws IOException {
    SocketChannel socketChannel = (SocketChannel) key.channel();
    int wrote = 0;

    final Queue<ByteBuffer> queue = writeBuffers.get(socketChannel);

    while (!queue.isEmpty()) {
      ByteBuffer buf = queue.poll();

      // write the size of the message
      ByteBuffer sizeBuf = ByteBuffer.allocate(4).putInt(0, buf.remaining());
      while (sizeBuf.hasRemaining()) {
        wrote += socketChannel.write(sizeBuf);
      }

      // write the message
      while (buf.hasRemaining()) {
        wrote += socketChannel.write(buf);
      }
    }
    assert wrote > 0;

    // set the key back to read-only so the selector will block
    selectorLock.lock();
    try {
      if (queue.isEmpty()) {
        key.interestOps(SelectionKey.OP_READ);
      }
    } finally {
      selectorLock.unlock();
    }
  }

  /** Adds a message to write to the specified channel. */
  public void addMessage(SelectionKey key, ByteBuffer data) {
    SocketChannel channel = (SocketChannel) key.channel();

    writeBuffers.get(channel).add(data);

    // lock the selector so we can modify it
    selectorLock.lock();
    try {
      selector.wakeup(); // wake up the selector from its slumber

      // make sure the channel is writable
      key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
    } finally {
      selectorLock.unlock();
    }
  }
}

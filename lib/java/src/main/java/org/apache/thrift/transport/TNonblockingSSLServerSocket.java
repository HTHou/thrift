/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.thrift.transport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import javax.net.ssl.SSLContext;
import org.apache.thrift.TConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Wrapper around ServerSocketChannel that negotiates TLS on accepted nonblocking sockets. */
public class TNonblockingSSLServerSocket extends TNonblockingServerTransport {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(TNonblockingSSLServerSocket.class.getName());

  private final ServerSocketChannel serverSocketChannel;
  private ServerSocket serverSocket;
  private final int clientTimeout;
  private final int maxFrameSize;
  private final int maxMessageSize;
  private final SSLContext sslContext;
  private final boolean clientAuth;
  private final String[] cipherSuites;

  public static class NonblockingSSLServerSocketArgs
      extends TNonblockingServerSocket.NonblockingAbstractServerSocketArgs {
    private SSLContext sslContext;
    private boolean clientAuth;
    private String[] cipherSuites;

    @Override
    public NonblockingSSLServerSocketArgs port(int port) {
      super.port(port);
      return this;
    }

    @Override
    public NonblockingSSLServerSocketArgs bindAddr(InetSocketAddress bindAddr) {
      super.bindAddr(bindAddr);
      return this;
    }

    @Override
    public NonblockingSSLServerSocketArgs clientTimeout(int clientTimeout) {
      super.clientTimeout(clientTimeout);
      return this;
    }

    @Override
    public NonblockingSSLServerSocketArgs backlog(int backlog) {
      super.backlog(backlog);
      return this;
    }

    @Override
    public NonblockingSSLServerSocketArgs maxFrameSize(int maxFrameSize) {
      super.maxFrameSize(maxFrameSize);
      return this;
    }

    @Override
    public NonblockingSSLServerSocketArgs maxMessageSize(int maxMessageSize) {
      super.maxMessageSize(maxMessageSize);
      return this;
    }

    public NonblockingSSLServerSocketArgs sslContext(SSLContext sslContext) {
      this.sslContext = sslContext;
      return this;
    }

    public NonblockingSSLServerSocketArgs clientAuth(boolean clientAuth) {
      this.clientAuth = clientAuth;
      return this;
    }

    public NonblockingSSLServerSocketArgs cipherSuites(String[] cipherSuites) {
      this.cipherSuites = cipherSuites;
      return this;
    }
  }

  public TNonblockingSSLServerSocket(int port, SSLContext sslContext) throws TTransportException {
    this(port, 0, TConfiguration.DEFAULT_MAX_FRAME_SIZE, sslContext, false, null);
  }

  public TNonblockingSSLServerSocket(int port, int clientTimeout, SSLContext sslContext)
      throws TTransportException {
    this(
        port,
        clientTimeout,
        TConfiguration.DEFAULT_MAX_FRAME_SIZE,
        sslContext,
        false,
        null);
  }

  public TNonblockingSSLServerSocket(
      int port,
      int clientTimeout,
      int maxFrameSize,
      SSLContext sslContext,
      boolean clientAuth,
      String[] cipherSuites)
      throws TTransportException {
    this(
        new NonblockingSSLServerSocketArgs()
            .port(port)
            .clientTimeout(clientTimeout)
            .maxFrameSize(maxFrameSize)
            .sslContext(sslContext)
            .clientAuth(clientAuth)
            .cipherSuites(cipherSuites));
  }

  public TNonblockingSSLServerSocket(InetSocketAddress bindAddr, SSLContext sslContext)
      throws TTransportException {
    this(bindAddr, 0, TConfiguration.DEFAULT_MAX_FRAME_SIZE, sslContext, false, null);
  }

  public TNonblockingSSLServerSocket(
      InetSocketAddress bindAddr,
      int clientTimeout,
      int maxFrameSize,
      SSLContext sslContext,
      boolean clientAuth,
      String[] cipherSuites)
      throws TTransportException {
    this(
        new NonblockingSSLServerSocketArgs()
            .bindAddr(bindAddr)
            .clientTimeout(clientTimeout)
            .maxFrameSize(maxFrameSize)
            .sslContext(sslContext)
            .clientAuth(clientAuth)
            .cipherSuites(cipherSuites));
  }

  public TNonblockingSSLServerSocket(NonblockingSSLServerSocketArgs args)
      throws TTransportException {
    if (args.sslContext == null) {
      throw new TTransportException("SSLContext is required for a nonblocking SSL server socket");
    }

    clientTimeout = args.clientTimeout;
    maxFrameSize = args.maxFrameSize;
    maxMessageSize = args.maxMessageSize;
    sslContext = args.sslContext;
    clientAuth = args.clientAuth;
    cipherSuites = args.cipherSuites;

    try {
      serverSocketChannel = ServerSocketChannel.open();
      serverSocketChannel.configureBlocking(false);

      serverSocket = serverSocketChannel.socket();
      serverSocket.setReuseAddress(true);
      serverSocket.bind(args.bindAddr, args.backlog);
    } catch (IOException ioe) {
      throw new TTransportException(
          "Could not create ServerSocket on address " + args.bindAddr + ".", ioe);
    }
  }

  @Override
  public void listen() throws TTransportException {
    if (serverSocket != null) {
      try {
        serverSocket.setSoTimeout(0);
      } catch (SocketException sx) {
        LOGGER.error("Socket exception while setting socket timeout", sx);
      }
    }
  }

  @Override
  public TNonblockingTransport accept() throws TTransportException {
    if (serverSocket == null) {
      throw new TTransportException(TTransportException.NOT_OPEN, "No underlying server socket.");
    }

    try {
      SocketChannel socketChannel = serverSocketChannel.accept();
      if (socketChannel == null) {
        return null;
      }

      TNonblockingSSLSocket socket =
          new TNonblockingSSLSocket(socketChannel, sslContext, clientAuth, cipherSuites);
      socket.setTimeout(clientTimeout);
      socket.setMaxFrameSize(maxFrameSize);
      socket.setMaxMessageSize(maxMessageSize);
      return socket;
    } catch (IOException iox) {
      throw new TTransportException(iox);
    }
  }

  @Override
  public void registerSelector(Selector selector) {
    try {
      serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
    } catch (ClosedChannelException e) {
      LOGGER.warn("Server socket channel was already closed", e);
    }
  }

  @Override
  public void close() {
    if (serverSocket != null) {
      try {
        serverSocket.close();
      } catch (IOException iox) {
        LOGGER.warn("Could not close server socket: {}", iox.getMessage());
      }
      serverSocket = null;
    }
  }

  @Override
  public void interrupt() {
    close();
  }

  public int getPort() {
    if (serverSocket == null) {
      return -1;
    }
    return serverSocket.getLocalPort();
  }

  ServerSocketChannel getServerSocketChannel() {
    return serverSocketChannel;
  }
}

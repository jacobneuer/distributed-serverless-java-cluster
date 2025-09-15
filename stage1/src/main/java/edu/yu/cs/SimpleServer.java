package edu.yu.cs;

public interface SimpleServer {
    //public SimpleServerImpl(int port) throws IOException

    /**
     * start the server
     */
    void start();

    /**
     * stop the server
     */
    void stop();
}
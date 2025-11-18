package edu.yu.cs.com3800.stage3;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.com3800.JavaRunner;
import edu.yu.cs.com3800.SimpleServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import java.util.logging.Logger;

public class SimpleServerImpl implements SimpleServer{

    private final HttpServer server;
    private static final Logger logger = Logger.getLogger(SimpleServerImpl.class.getName());

    public SimpleServerImpl(int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/compileandrun", new CompileAndRunHandler());
    }

    private static class CompileAndRunHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            // Enforce POST request
            if (!exchange.getRequestMethod().equalsIgnoreCase("POST")) {
                exchange.getResponseHeaders().set("Allow", "POST");
                exchange.sendResponseHeaders(405, -1); // 405 Method Not Allowed
                return;
            }

            // Enforce Content Type
            String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
            if (contentType == null || !contentType.equalsIgnoreCase("text/x-java-source")) {
                exchange.sendResponseHeaders(400, -1); // 400 Bad Request
                return;
            }

            // Call JavaRunner with the request body InputStream
            try (InputStream in = exchange.getRequestBody()) {
                JavaRunner runner = new JavaRunner();
                String result = runner.compileAndRun(in);

                // Successful execution: 200 with result
                send(exchange, 200, result);
            } catch (Exception e) {
                // Failure during compilation or execution: 400 with error message
                send(exchange, 400, exceptionPayload(e));
            }
        }

        private static String exceptionPayload(Exception e) {
            String errorMessage = (e.getMessage() == null) ? "" : e.getMessage();
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            try (PrintStream ps = new PrintStream(outputStream, true, StandardCharsets.UTF_8)) {
                e.printStackTrace(ps);
            }
            String stack = outputStream.toString(StandardCharsets.UTF_8);
            return errorMessage + "\n" + stack;
        }

        private static void send(HttpExchange exchange, int status, String body) throws IOException {
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=UTF-8");
            exchange.sendResponseHeaders(status, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        }

    }

    public static void main(String[] args)
    {
        int port = 9000;
        if(args.length >0)
        {
            port = Integer.parseInt(args[0]);
        }
        SimpleServer myserver = null;
        try
        {
            myserver = new SimpleServerImpl(port);
            myserver.start();
        }
        catch(Exception e)
        {
            System.err.println(e.getMessage());
            myserver.stop();
        }
    }

    /**
     * start the server
     */
    @Override
    public void start() {
        server.start();
        logger.info(() -> "Server started on port " + server.getAddress().getPort());
    }

    /**
     * stop the server
     */
    @Override
    public void stop() {
        server.stop(0);
        logger.info("Server stopped.");
    }

}

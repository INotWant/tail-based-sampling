package edu.bupt.linktracking;

import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

public class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class.getName());

    public static volatile int LISTEN_PORT;
    public static volatile int DATA_SOURCE_PORT;

    private static String responseStr = "HTTP/1.1 200\r\n" +
            "Content-Type: text/plain;charset=UTF-8\r\n" +
            "Content-Length: 3\r\n" +
            "Connection: close\r\n" +
            "\r\n" +
            "suc";

    public static void responseDataSourceReady(Socket dataSourceSocket) throws IOException {
        BufferedOutputStream bos = new BufferedOutputStream(dataSourceSocket.getOutputStream());

        // response: ready
        byte[] responseBytes = responseStr.getBytes();
        bos.write(responseBytes);
        bos.flush();
        dataSourceSocket.close();
    }

    public static int getDataSourcePort(Socket dataSourceSocket) throws IOException {
        int datSourceSocket = -1;
        BufferedReader bfReader = new BufferedReader(new InputStreamReader(dataSourceSocket.getInputStream()));
        BufferedOutputStream bos = new BufferedOutputStream(dataSourceSocket.getOutputStream());

        // read: HTTP 1.1 Get localhost:{8000,8001,8002}/setParameter
        String line;
        int index;
        while ((line = bfReader.readLine()) != null) {
            index = line.indexOf("port");
            if (index > -1) {
                datSourceSocket = Integer.parseInt(line.substring(index + 5, line.lastIndexOf(' ')));
                break;
            }
        }

        // response: setParameter
        byte[] responseBytes = responseStr.getBytes();
        bos.write(responseBytes);
        bos.flush();

        dataSourceSocket.close();
        return datSourceSocket;
    }

    public static void main(String[] args) {
        String portStr = System.getProperty("server.port", "8080");
        LISTEN_PORT = Integer.parseInt(portStr);

        if (Utils.isClientProcess()) {
            LOGGER.info("start a client, port: " + LISTEN_PORT);
            new Thread(new edu.bupt.linktracking.clientprocess.MainRunnable()).start();
        } else if (Utils.isBackendProcess()) {
            LOGGER.info("start a backend, port: " + LISTEN_PORT);
            new Thread(new edu.bupt.linktracking.backendprocess.MainRunnable()).start();
        }
    }

}

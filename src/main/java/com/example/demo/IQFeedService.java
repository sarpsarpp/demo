package com.example.demo;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

@Service
public class IQFeedService {

    private final String HOST = "127.0.0.1";
    private final int PORT = 9300;
    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;

    @PostConstruct
    public void init() throws IOException {
        // Establish a TCP/IP connection to IQFeed's IQConnect
        socket = new Socket(HOST, PORT);
        out = new PrintWriter(socket.getOutputStream(), true);
        in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        // Set the protocol version
        setProtocol("6.2");

        // Register your application with the feed
        registerClientApp("SARP_GUVEN_50892", "1");
    }

    public void setProtocol(String version) throws IOException {
        String command = String.format("S,SET PROTOCOL,%s\r\n", version);
        out.println(command);

        String response = in.readLine();
        System.out.println(response);  // Log the response
    }

    public void registerClientApp(String registeredProductId, String productVersion) throws IOException {
        String command = String.format("S,REGISTER CLIENT APP,%s,%s\r\n", registeredProductId, productVersion);
        out.println(command);

        String response = in.readLine();
        System.out.println(response);  // Log the response
    }

    @PreDestroy
    public void cleanup() throws IOException {
        // Close the resources
        in.close();
        out.close();
        socket.close();
    }
}

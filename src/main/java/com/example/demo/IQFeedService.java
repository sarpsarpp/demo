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
    private final int ADMIN_PORT = 9300;
    private final int DATA_PORT = 5009;

    private Socket adminSocket;
    private Socket dataSocket;
    private PrintWriter adminOut;
    private PrintWriter dataOut;
    private BufferedReader adminIn;
    private BufferedReader dataIn;

    @PostConstruct
    public void init() throws IOException {
        // Establish a TCP/IP connection to IQFeed's IQConnect
        adminSocket = new Socket(HOST, ADMIN_PORT);
        adminOut = new PrintWriter(adminSocket.getOutputStream(), true);
        adminIn = new BufferedReader(new InputStreamReader(adminSocket.getInputStream()));
        dataSocket = new Socket(HOST, DATA_PORT);
        dataOut = new PrintWriter(dataSocket.getOutputStream(), true);
        dataIn = new BufferedReader(new InputStreamReader(dataSocket.getInputStream()));
        new Thread(this::readResponses).start();

        // Set the protocol version
        setProtocol("6.2");
        registerClientApp("SARP_GUVEN_50892", "1");



        // Register your application with the feed

        requestData("AAPL");
    }

    public void setProtocol(String version) throws IOException {
        String command = String.format("S,SET PROTOCOL,%s\r\n", version);
        adminOut.println(command);

        String response = adminIn.readLine();
        System.out.println(response);  // Log the response
    }

    public void requestData(String symbol) throws IOException {
        String command = String.format("w%s\r\n", symbol);
        dataOut.println(command);
        dataOut.flush();  // Ensure the command is sent immediately

        // Assuming that the data will be received on the next line
        // (check the IQFeed documentation to confirm how the data will be sent)
        String response = dataIn.readLine();
        System.out.println(response);  // This will print the response to the console
    }

    public void registerClientApp(String registeredProductId, String productVersion) throws IOException {
        String command = String.format("S,REGISTER CLIENT APP,%s,%s\r\n", registeredProductId, productVersion);
        adminOut.println(command);

        String response = adminIn.readLine();
        System.out.println(response);  // Log the response
    }

    public void readResponses() {
        String line;
        try {
            while ((line = dataIn.readLine()) != null) {
                System.out.println(line);  // Print each response line to the console
            }
        } catch (IOException e) {
            e.printStackTrace();  // Print any exceptions to the console
        }
    }

    @PreDestroy
    public void cleanup() throws IOException {
        // Close the resources
        adminIn.close();
        adminOut.close();
        adminSocket.close();

        dataIn.close();
        dataOut.close();
        dataSocket.close();
    }
}

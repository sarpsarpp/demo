package com.example.demo;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.*;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
    private Path dir;
    private Map<String, Double> optionValues = new HashMap<>();
    private final ExecutorService executorService = Executors.newFixedThreadPool(10);  // Create a thread pool with 10 threads

    @Autowired
    public IQFeedService(){
        String os = System.getProperty("os.name").toLowerCase();
        if (os.contains("win")){
            this.dir = Paths.get("C:\\ftgt\\ALI_INDVAL");
        }
        else if (os.contains("mac")){
            this.dir =  Paths.get("/Users/sarpguven/Desktop/trading/ftgt/ALI_INDVAL");
        }
    }
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
        updateFields();
        // Register your application with the feed

        executeReqs();
    }

    public void setProtocol(String version) throws IOException {
        String command = String.format("S,SET PROTOCOL,%s\r\n", version);
        adminOut.println(command);

        String response = adminIn.readLine();
        System.out.println(response);  // Log the response
    }
    public void executeReqs(){
        executorService.submit(() -> {
            try {
                requestData("DCORN.Z");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        executorService.submit(() -> {
            try {
                requestData("DPORN.Z");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        executorService.submit(() -> {
            try {
                requestData("TCORA.Z");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        executorService.submit(() -> {
            try {
                requestData("TCORD.Z");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        executorService.submit(() -> {
            try {
                requestData("TPORA.Z");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        executorService.submit(() -> {
            try {
                requestData("TPORD.Z");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

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

    public void updateFields() throws IOException {
        String command = "S,SELECT UPDATE FIELDS,Last,Close\r\n";
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
        System.out.println("read responses");

        String line;
        try {
            while ((line = dataIn.readLine()) != null) {
                //System.out.println(line);  // Print each response line to the console
                if (line.startsWith("Q,")) {
                    processQMessage(line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();  // Print any exceptions to the console
        }
    }

    private void processQMessage(String message) {
        // Split the message by comma
        String[] parts = message.split(",");
        System.out.println("process q");
        // Ensure the message has at least three parts (Q, name, value)
        if (parts.length >= 3) {
            String name = parts[1];
            String valueString = parts[2];
            try{
                Double value = Double.parseDouble(valueString);
                this.optionValues.put(name, value);
            }catch (NumberFormatException e) {
                System.err.println("Failed to parse value as double for message: " + message);
                e.printStackTrace();  // Print the exception to the console
            }
        }
    }

    @Scheduled(fixedRate = 15000)
    public void writeTable(){
        String outputFileName = this.dir + "/Bookmap/OptionCalculations.txt";
        System.out.println("write table");
        try {
            // Use BufferedWriter for efficient writing
            BufferedWriter writer = new BufferedWriter(new FileWriter(outputFileName));
            // Write VIX values
            for (String key : this.optionValues.keySet()) {
                writer.write(key + ", " + optionValues.get(key));
                writer.newLine();
            }
            // Always close the writer when done
            writer.close();

        } catch(IOException e) {
            // Handle exceptions
            e.printStackTrace();
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

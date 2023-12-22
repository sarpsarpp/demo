package com.example.demo;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.*;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@EnableScheduling
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
    private final Map<String, List<String>> optionValues = new HashMap<>();
    private final ExecutorService executorService = Executors.newFixedThreadPool(10);  // Create a thread pool with 10 threads
    private final String es = "@ESH24";
    private final String vixFutures = "@VX#";//
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
        executorService.submit(() -> {
            try {
                requestData("VIX1D.XO");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        executorService.submit(() -> {
            try {
                requestData("VIX9D.XO");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        executorService.submit(() -> {
            try {
                requestData("VIX.XO");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        executorService.submit(() -> {
            try {
                requestData(vixFutures);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        executorService.submit(() -> {
            try {
                requestData(es);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        executorService.submit(() -> {
            try {
                requestData("PROM.Z");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        executorService.submit(() -> {
            try {
                requestData("VXN.XO");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        executorService.submit(() -> {
            try {
                requestData("SPIKE.X");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        executorService.submit(() -> {
            try {
                requestData("VXGOG.XO");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        executorService.submit(() -> {
            try {
                requestData("VXAPL.XO");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        executorService.submit(() -> {
            try {
                requestData("VXAZN.XO");
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
        String command = "S,SELECT UPDATE FIELDS,Last,Percent Change,Change From Open\r\n";//percent change for @ES
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
                System.out.println(line);  // Print each response line to the console
                if (line.startsWith("Q,")) {
                    processQMessage(line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();  // Print any exceptions to the console
        }
    }

    private void processQMessage(String message) {
        String[] parts = message.split(",");
        System.out.println("process q");
        if (parts.length >= 3) {
            String name = parts[1];
            List<String> values = new ArrayList<>();
            for (int i = 2; i < parts.length; i++) {
                try {
                    //Double value = Double.parseDouble(parts[i]);
                    values.add(parts[i]);
                } catch (NumberFormatException e) {
                    System.err.println("Failed to parse value as double for message: " + message);
                    e.printStackTrace();
                }
            }
            this.optionValues.put(name, values);
        }
    }

    @Scheduled(fixedRate = 15000)
    public void writeTable() {
        String outputFileName = this.dir + "/OptionCalculations.txt";
        System.out.println("write table");
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(outputFileName));
            for (String key : this.optionValues.keySet()) {
                List<String> values = this.optionValues.get(key);
                String valuesString = values.stream().map(String::valueOf).collect(Collectors.joining(", "));
                if(key.equals(vixFutures)){
                    writer.write("vixFutures" + ", " + valuesString);
                }else{
                    writer.write(key + ", " + valuesString);
                }
                writer.newLine();
            }
            writer.close();
        } catch(IOException e) {
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

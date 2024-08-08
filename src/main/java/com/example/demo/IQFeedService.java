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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
    private final Map<String, List<String>> optionValues = new ConcurrentHashMap<>();
    private final ExecutorService executorService = Executors.newFixedThreadPool(10);
    private final String es = "@ESU24";
    private final String vixFutures = "@VX#";

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
        adminSocket = new Socket(HOST, ADMIN_PORT);
        adminOut = new PrintWriter(adminSocket.getOutputStream(), true);
        adminIn = new BufferedReader(new InputStreamReader(adminSocket.getInputStream()));
        dataSocket = new Socket(HOST, DATA_PORT);
        dataOut = new PrintWriter(dataSocket.getOutputStream(), true);
        dataIn = new BufferedReader(new InputStreamReader(dataSocket.getInputStream()));
        new Thread(this::readResponses).start();

        setProtocol("6.2");
        registerClientApp("SARP_GUVEN_50892", "1");
        updateFields();
        executeReqs();
    }

    public void setProtocol(String version) throws IOException {
        String command = String.format("S,SET PROTOCOL,%s\r\n", version);
        adminOut.println(command);
        String response = adminIn.readLine();
        System.out.println(response);
    }

    public void registerClientApp(String registeredProductId, String productVersion) throws IOException {
        String command = String.format("S,REGISTER CLIENT APP,%s,%s\r\n", registeredProductId, productVersion);
        adminOut.println(command);
        String response = adminIn.readLine();
        System.out.println(response);
    }
    public void updateFields() throws IOException {
        String command = "S,SELECT UPDATE FIELDS,Last,Percent Change,Change From Open\r\n";
        dataOut.println(command);
        dataOut.flush();
        String response = dataIn.readLine();
        System.out.println(response);
    }

    public void executeReqs(){
        String[] symbols = {
                "VIX1D.XO", "DCORN.Z", "DPORN.Z", "TCORA.Z", "TCORD.Z", "TPORA.Z", "TPORD.Z",
                "VIX9D.XO", "VIX.XO", "PROM.Z",
                "VXN.XO", "SPIKE.X", "VXGOG.XO", "VXAPL.XO", "VXAZN.XO", "VXGS.XO",
                "JV6T.Z", "JV5T.Z", "JV1T.Z", "JVRT.Z", "DI6N.Z", "DI5N.Z", "DI1N.Z",
                "DIRN.Z", "VCORA.Z", "VCORD.Z", "VPORA.Z", "VPORD.Z", "DCORA.Z",
                "DCORD.Z", "DPORA.Z", "DPORD.Z", "II6A.Z", "II6D.Z", "M206V.Z",
                "M206B.Z", "VI6A.Z", "VI6D.Z", "VI5A.Z", "VI5D.Z", "VI1A.Z", "VI1D.Z", vixFutures, es,
        };

        for (String symbol : symbols) {
            executorService.submit(() -> {
                try {
                    requestData(symbol);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    public void requestData(String symbol) throws IOException {
        String command = String.format("w%s\r\n", symbol);
        dataOut.println(command);
        dataOut.flush();
        String response = dataIn.readLine();
        System.out.println(response);
    }



    public void readResponses() {
        System.out.println("read responses");

        String line;
        try {
            while ((line = dataIn.readLine()) != null) {
                System.out.println(line);
                if (line.startsWith("Q,")) {
                    processQMessage(line);
                }
                if (line.startsWith("P,")) {
                    processPMessage(line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void processPMessage(String message) {
        String[] parts = message.split(",");
        System.out.println("process p");
        if (parts.length >= 3){
            String name = parts[1];
            List<String> values = new ArrayList<>();
            try{
                String last = parts[3];
                String percentChange = "0";
                String changeFromOpen = "0";
                values.add(last);
                values.add(percentChange);
                values.add(changeFromOpen);
            }catch (NumberFormatException e){
                System.err.println("Failed to parse value as double for message: " + message);
                e.printStackTrace();
            }
            optionValues.put(name, values);
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
                    values.add(parts[i]);
                } catch (NumberFormatException e) {
                    System.err.println("Failed to parse value as double for message: " + message);
                    e.printStackTrace();
                }
            }
            optionValues.put(name, values);
        }
    }

    @Scheduled(fixedRate = 30000)
    public void writeTable() {
        String outputFileName = this.dir + "/OptionCalculations.txt";
        System.out.println("write table");
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(outputFileName));
            for (String key : optionValues.keySet()) {
                List<String> values = optionValues.get(key);
                String valuesString = values.stream().map(String::valueOf).collect(Collectors.joining(", "));
                if(key.equals(vixFutures)){
                    writer.write("vixFutures" + ", " + valuesString);
                }
                else{
                    writer.write(key + ", " + valuesString);
                }
                writer.newLine();
            }
            writer.close();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    @Scheduled(cron = "0 0 17 * * *", zone = "America/New_York")
    public void saveToFile(){
        String outputFileName = this.dir + "/OptionCalculations" + java.time.LocalDate.now() + ".txt";
        System.out.println("save to file");
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(outputFileName));
            for (String key : optionValues.keySet()) {
                List<String> values = optionValues.get(key);
                String valuesString = values.stream().map(String::valueOf).collect(Collectors.joining(", "));
                if(key.equals(vixFutures)){
                    writer.write("vixFutures" + ", " + valuesString);
                } else {
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
        adminIn.close();
        adminOut.close();
        adminSocket.close();
        dataIn.close();
        dataOut.close();
        dataSocket.close();
    }
}

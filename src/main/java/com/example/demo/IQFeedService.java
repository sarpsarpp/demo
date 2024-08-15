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
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@EnableScheduling
@Service
public class IQFeedService {

    private static final String HOST = "127.0.0.1";
    private static final int ADMIN_PORT = 9300;
    private static final int DATA_PORT = 5009;
    private static final List<String> SYMBOLS = Arrays.asList(
            "DCORN.Z", "DPORN.Z", "TCORA.Z", "TCORD.Z", "TPORA.Z", "TPORD.Z", "VIX1D.XO", "VIX9D.XO", "VIX.XO",
            "@VX#", "@ESU24", "PROM.Z", "VXN.XO", "SPIKE.X", "VXGOG.XO", "VXAPL.XO", "VXAZN.XO", "VXGS.XO",
            "JV6T.Z", "JV5T.Z", "JV1T.Z", "JVRT.Z", "DI6N.Z", "DI5N.Z", "DI1N.Z", "DIRN.Z", "VCORA.Z",
            "VCORD.Z", "VPORA.Z", "VPORD.Z", "DCORA.Z", "DCORD.Z", "DPORA.Z", "DPORD.Z", "II6A.Z", "II6D.Z",
            "M206V.Z", "M206B.Z", "VI6A.Z", "VI6D.Z", "VI5A.Z", "VI5D.Z", "VI1A.Z", "VI1D.Z"
    );

    private Socket adminSocket;
    private Socket dataSocket;
    private PrintWriter adminOut;
    private PrintWriter dataOut;
    private BufferedReader adminIn;
    private BufferedReader dataIn;
    private Path dir;
    private final Map<String, List<String>> optionValues = new ConcurrentHashMap<>();
    private final ExecutorService executorService = Executors.newFixedThreadPool(10);

    @Autowired
    public IQFeedService() {
        String os = System.getProperty("os.name").toLowerCase();
        this.dir = os.contains("win") ?
                Paths.get("C:\\ftgt\\ALI_INDVAL") :
                Paths.get("/Users/sarpguven/Desktop/trading/ftgt/ALI_INDVAL");
    }

    @PostConstruct
    public void init() throws IOException {
        establishConnections();
        setProtocol("6.2");
        registerClientApp("SARP_GUVEN_50892", "1");
        updateFields();
        requestSymbolsData();
    }

    private void establishConnections() throws IOException {
        adminSocket = new Socket(HOST, ADMIN_PORT);
        adminOut = new PrintWriter(adminSocket.getOutputStream(), true);
        adminIn = new BufferedReader(new InputStreamReader(adminSocket.getInputStream()));

        dataSocket = new Socket(HOST, DATA_PORT);
        dataOut = new PrintWriter(dataSocket.getOutputStream(), true);
        dataIn = new BufferedReader(new InputStreamReader(dataSocket.getInputStream()));

        executorService.submit(this::readResponses);
    }

    public void setProtocol(String version) throws IOException {
        sendAdminCommand(String.format("S,SET PROTOCOL,%s\r\n", version));
    }

    public void registerClientApp(String registeredProductId, String productVersion) throws IOException {
        sendAdminCommand(String.format("S,REGISTER CLIENT APP,%s,%s\r\n", registeredProductId, productVersion));
    }

    public void updateFields() throws IOException {
        sendDataCommand("S,SELECT UPDATE FIELDS,Last,Percent Change,Change From Open\r\n");
    }

    private void sendAdminCommand(String command) throws IOException {
        adminOut.println(command);
        logResponse(adminIn.readLine());
    }

    private void sendDataCommand(String command) throws IOException {
        dataOut.println(command);
        dataOut.flush();
        logResponse(dataIn.readLine());
    }

    private void logResponse(String response) {
        System.out.println(response); // Use a logger in production
    }

    private void requestSymbolsData() {
        SYMBOLS.forEach(symbol -> executorService.submit(() -> {
            try {
                requestData(symbol);
            } catch (IOException e) {
                e.printStackTrace(); // Improve error handling for robustness
            }
        }));
    }

    public void requestData(String symbol) throws IOException {
        sendDataCommand(String.format("w%s\r\n", symbol));
    }

    private void readResponses() {
        String line;
        try {
            while ((line = dataIn.readLine()) != null) {
                if (line.startsWith("Q,")) {
                    processQMessage(line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void processQMessage(String message) {
        String[] parts = message.split(",");
        if (parts.length >= 3) {
            String name = parts[1];
            List<String> values = Arrays.asList(Arrays.copyOfRange(parts, 2, parts.length));
            optionValues.put(name, values);
        }
    }

    @Scheduled(fixedRate = 15000)
    public void writeTable() {
        String outputFileName = this.dir + "/OptionCalculations.txt";
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFileName))) {
            for (Map.Entry<String, List<String>> entry : optionValues.entrySet()) {
                String key = entry.getKey().equals("@VX#") ? "vixFutures" : entry.getKey();
                String valuesString = String.join(", ", entry.getValue());
                writer.write(key + ", " + valuesString);
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @PreDestroy
    public void cleanup() {
        closeResource(adminIn);
        closeResource(adminOut);
        closeResource(adminSocket);

        closeResource(dataIn);
        closeResource(dataOut);
        closeResource(dataSocket);

        executorService.shutdown();
    }

    private void closeResource(Closeable resource) {
        if (resource != null) {
            try {
                resource.close();
            } catch (IOException e) {
                e.printStackTrace(); // Replace with logger in production
            }
        }
    }

    @Scheduled(cron = "0 30 16 * * ?", zone = "America/New_York")
    public void saveDailyOptionCalculations() {
        LocalDateTime now = LocalDateTime.now(ZoneId.of("America/New_York"));
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("ddMMyy");
        String fileName = "OptionCalculations" + now.format(dateFormatter) + ".txt";
        Path savePath = Paths.get(dir.toString(), "save", fileName);

        File saveDir = savePath.getParent().toFile();
        if (!saveDir.exists()) {
            saveDir.mkdirs(); // Create the save directory if it doesn't exist
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(savePath.toFile()))) {
            for (Map.Entry<String, List<String>> entry : optionValues.entrySet()) {
                String key = entry.getKey().equals("@VX#") ? "vixFutures" : entry.getKey();
                String valuesString = String.join(", ", entry.getValue());
                writer.write(key + ", " + valuesString);
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace(); // Replace with a logger in production
        }
    }
}

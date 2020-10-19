package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Exchanger;

import static java.lang.Thread.currentThread;
import static org.slf4j.LoggerFactory.getLogger;

public class FileWriter
        implements Runnable {

    private static final Logger logger = getLogger(FileWriter.class);
    private final Path resultFilePath;
    private final Exchanger<Map<String, Pair<String, Integer>>> exchanger;

    public FileWriter(String resultFileName, Exchanger<Map<String, Pair<String, Integer>>> exchanger) {
        this.resultFilePath = Path.of(resultFileName);
        this.exchanger = exchanger;
    }

    @Override
    public void run() {
        logger.info("Started writer thread {}", currentThread().getName());
        try (OutputStream os = Files.newOutputStream(resultFilePath, StandardOpenOption.CREATE);
             PrintWriter fileWriter = new PrintWriter(os)) {
            while (true) {
                Map<String, Pair<String, Integer>> map = new HashMap<>();
                try {
                    map = exchanger.exchange(map);
                } catch (InterruptedException e) {
                    break;
                }
                for (Map.Entry<String, Pair<String, Integer>> entry : map.entrySet()) {
                    String line = entry.getValue().toString("%s %s");
                    fileWriter.println(line);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        logger.info("Finish writer thread {}", currentThread().getName());
    }
}

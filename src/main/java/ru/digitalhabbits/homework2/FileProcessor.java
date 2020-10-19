package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static java.lang.Runtime.getRuntime;
import static java.nio.charset.Charset.defaultCharset;
import static org.slf4j.LoggerFactory.getLogger;

public class FileProcessor {
    private static final Logger logger = getLogger(FileProcessor.class);
    public static final int CHUNK_SIZE = 2 * getRuntime().availableProcessors();
    private static final Phaser PHASER = new Phaser(CHUNK_SIZE + 1);

    public void process(@Nonnull String processingFileName, @Nonnull String resultFileName) {
        checkFileExists(processingFileName);
        final File file = new File(processingFileName);

        ExecutorService lineProcessingExecutor = Executors.newFixedThreadPool(CHUNK_SIZE);

        Exchanger<Map<String, Pair<String, Integer>>> exchanger = new Exchanger<>();
        Thread writerThread = new Thread(new FileWriter(resultFileName, exchanger));
        writerThread.start();

        try (final Scanner scanner = new Scanner(file, defaultCharset())) {
            while (scanner.hasNext()) {
                LinkedHashMap<String, Pair<String, Integer>> map = new LinkedHashMap<>();
                for (int i = 0; i < CHUNK_SIZE; i++) {
                    if (scanner.hasNext()) {
                        map.put(scanner.nextLine(), null);
                    } else {
                        for (; i < CHUNK_SIZE; i++) {
                            PHASER.arriveAndDeregister();
                        }
                        break;
                    }
                }

                for (String string : map.keySet()) {
                    lineProcessingExecutor.submit(() -> {
                        Pair<String, Integer> pair = new LineCounterProcessor().process(string);
                        map.put(pair.getLeft(), pair);
                        PHASER.arriveAndAwaitAdvance();
                    });
                }
                PHASER.arriveAndAwaitAdvance();

                try {
                    exchanger.exchange(map);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (IOException exception) {
            logger.error("", exception);
        }

        writerThread.interrupt();
        lineProcessingExecutor.shutdown();
        logger.info("Finish main thread {}", Thread.currentThread().getName());
    }

    private void checkFileExists(@Nonnull String fileName) {
        final File file = new File(fileName);
        if (!file.exists() || file.isDirectory()) {
            throw new IllegalArgumentException("File '" + fileName + "' not exists");
        }
    }
    
    public static void main(String[] args) {
        FileProcessor fileProcessor = new FileProcessor();
        String in = "D:\\Java\\digital habits\\homework2\\integration-test\\An Imitation of Spenser\\text.txt";
        String out = "D:\\Java\\digital habits\\homework2\\integration-test\\An Imitation of Spenser\\result1.txt";
        fileProcessor.process(in, out);
    }
}

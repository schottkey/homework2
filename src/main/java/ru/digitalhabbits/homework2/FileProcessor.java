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

    public void process(@Nonnull String processingFileName, @Nonnull String resultFileName) {
        checkFileExists(processingFileName);
        final File file = new File(processingFileName);

        final Phaser phaser = new Phaser(CHUNK_SIZE + 1);
        ExecutorService lineProcessingExecutor = Executors.newFixedThreadPool(CHUNK_SIZE);

        Exchanger<String[]> exchanger = new Exchanger<>();
        Thread writerThread = new Thread(new FileWriter(resultFileName, exchanger));
        writerThread.start();

        try (final Scanner scanner = new Scanner(file, defaultCharset())) {
            while (scanner.hasNext()) {
                final List<String> lines = new ArrayList<>();
                for (int i = 0; i < CHUNK_SIZE; i++) {
                    if (scanner.hasNext()) {
                        lines.add(scanner.nextLine());
                    } else {
                        for (; i < CHUNK_SIZE; i++) {
                            phaser.arriveAndDeregister();
                        }
                        break;
                    }
                }

                final String[] resultLines = new String[lines.size()];
                for (int i = 0; i < lines.size(); i++) {
                    final int index = i;
                    final String line = lines.get(i);

                    lineProcessingExecutor.submit(() -> {
                        Pair<String, Integer> pair = new LineCounterProcessor().process(line);
                        resultLines[index] = pair.toString("%s %s");
                        phaser.arrive();
                    });
                }
                phaser.arriveAndAwaitAdvance();

                try {
                    exchanger.exchange(resultLines);
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
}

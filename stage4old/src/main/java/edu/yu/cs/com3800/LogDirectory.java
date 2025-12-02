package edu.yu.cs.com3800;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class LogDirectory {

    private static final String ROOT = "logs";
    private static final String RUN_FOLDER;

    static {
        // Create root folder
        new File(ROOT).mkdirs();

        // Create a unique folder per run
        String timestamp = LocalDateTime.now().format(
                DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss")
        );
        RUN_FOLDER = ROOT + "/run-" + timestamp;
        new File(RUN_FOLDER).mkdirs();
    }

    public static String getRunDirectory() {
        return RUN_FOLDER;
    }
}

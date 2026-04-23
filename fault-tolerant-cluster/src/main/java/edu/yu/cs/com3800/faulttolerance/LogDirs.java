package edu.yu.cs.com3800.faulttolerance;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public final class LogDirs {
    public static final String LOG_DIR;

    static {
        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd-kk_mm");
        LOG_DIR = "logs-" + LocalDateTime.now().format(fmt);
        new File(LOG_DIR).mkdirs();
    }
}

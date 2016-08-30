package com.onevizion.scmdb;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;
import org.slf4j.LoggerFactory;

public class ColorLogger {
    private Logger logger = (Logger) LoggerFactory.getLogger("STDOUT");
    private PatternLayoutEncoder encoder;


    public ColorLogger() {
        LoggerContext loggerContext = logger.getLoggerContext();
        loggerContext.reset();

        encoder = new PatternLayoutEncoder();
        encoder.setContext(loggerContext);
        encoder.setPattern("%msg%n");
        encoder.start();

        ConsoleAppender<ILoggingEvent> appender = new ConsoleAppender<ILoggingEvent>();
        appender.setContext(loggerContext);
        appender.setEncoder(encoder);
        appender.start();

        logger.addAppender(appender);

        encoder.stop();
        encoder.setPattern("%cyan(%message%n)");
        encoder.start();
    }

    public void info(String msg, Color color) {
        encoder.stop();
        encoder.setPattern("%" + color.getColor() + "(%message%n)");
        encoder.start();

        logger.info(msg);
    }

    public enum Color {
        CYAN("cyan"),
        RED("red"),
        GREEN("green");

        private String color;

        Color(String color) {
            this.color = color;
        }

        public String getColor() {
            return color;
        }
    }
}

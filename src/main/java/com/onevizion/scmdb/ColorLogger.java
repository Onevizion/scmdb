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

        ConsoleAppender<ILoggingEvent> appender = new ConsoleAppender<>();
        appender.setContext(loggerContext);
        appender.setEncoder(encoder);
        appender.start();

        logger.addAppender(appender);
    }

    public void info(String msg, Color color, Object... argArray) {
        encoder.stop();
        encoder.setPattern("%" + color.getColor() + "(%message%n)");
        encoder.start();

        logger.info(msg, argArray);
    }

    public void info(String msg, Object... argArray) {
        info(msg, Color.WHITE, argArray);
    }

    public void warn(String msg, Color color, Object... argArray) {
        encoder.stop();
        encoder.setPattern("%" + color.getColor() + "(%message%n)");
        encoder.start();

        logger.warn(msg, argArray);
    }

    public void error(String msg, Object... argArray) {
        encoder.stop();
        encoder.setPattern("%" + Color.RED.getColor() + "(%message%n)");
        encoder.start();

        logger.error(msg, argArray);
    }

    public enum Color {
        WHITE("boldWhite"),
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

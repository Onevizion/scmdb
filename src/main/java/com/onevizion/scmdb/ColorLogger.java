package com.onevizion.scmdb;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;


public class ColorLogger {
    private Logger logger = (Logger) LoggerFactory.getLogger("STDOUT");
    private PatternLayoutEncoder encoder;

    @Autowired
    private AppArguments appArguments;

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
        setColor(color);
        logger.info(msg, argArray);
    }

    public void info(String msg, Object... argArray) {
        info(msg, Color.WHITE, argArray);
    }

    public void warn(String msg, Color color, Object... argArray) {
        setColor(color);
        logger.warn(msg, argArray);
    }

    public void error(String msg, Object... argArray) {
        setColor(Color.RED);
        logger.error(msg, argArray);
    }

    private void setColor(Color color) {
        if (appArguments.isUseColorLogging()) {
            encoder.stop();
            encoder.setPattern("%" + color.getColor() + "(%message%n)");
            encoder.start();
        }
    }

    public void debug(String msg, Object... argArray) {
        setColor(Color.WHITE);
        logger.debug(msg, argArray);
    }

    public enum Color {
        WHITE("white"),
        CYAN("cyan"),
        YELLOW("yellow"),
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

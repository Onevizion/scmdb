package com.onevizion.scmdb;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;

public class TextFilePopulator {
    private static final Logger logger = LoggerFactory.getLogger(TextFilePopulator.class);

    public static void populate(@SuppressWarnings("rawtypes") Class clazz) {
        for (Field field : clazz.getDeclaredFields()) {
            if (String.class == field.getType()) {
                for (Annotation annot : field.getDeclaredAnnotations()) {
                    if (TextFile.class == annot.annotationType()) {
                        String path = ((TextFile) annot).value();
                        String sql;
                        try {
                            sql = IOUtils.toString(new ClassPathResource(path).getInputStream());
                        } catch (IOException e) {
                            logger.error("Can't load text from [{}]", path, e);
                            throw new RuntimeException(e);
                        }
                        try {
                            field.setAccessible(true);
                            field.set(null, sql);
                        } catch (IllegalArgumentException e) {
                            logger.error("Can't set value on [{}.{}]", clazz, field.getName(), e);
                            throw new RuntimeException(e);
                        } catch (IllegalAccessException e) {
                            logger.error("Can't set value on [{}.{}]", clazz, field.getName(), e);
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        }
    }
}
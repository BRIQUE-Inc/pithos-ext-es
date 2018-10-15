package org.chronotics.pithos.ext.es.log;

import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

public class LogbackLogger implements Logger {
    private final ch.qos.logback.classic.Logger log;

    public LogbackLogger(Class<?> clazz) {
        log = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(clazz);
    }

    @Override
    public String getName() {
        return log.getName();
    }

    @Override
    public boolean isTraceEnabled() {
        return log.isTraceEnabled();
    }

    @Override
    public void trace(String msg) {
        log.trace(msg);
    }

    @Override
    public void trace(String format, Object arg) {
        // do not support
    }

    @Override
    public void trace(String format, Object... arguments) {
        // do not support
    }

    @Override
    public void trace(String msg, Throwable t) {
        log.trace(msg, t);
    }

    @Override
    public boolean isTraceEnabled(Marker marker) {
        return false;
    }

    @Override
    public void trace(Marker marker, String msg) {
        // do not support
    }

    @Override
    public void trace(Marker marker, String format, Object arg) {
        // do not support
    }

    @Override
    public void trace(Marker marker, String format, Object... argArray) {
        // do not support
    }

    @Override
    public void trace(Marker marker, String msg, Throwable t) {
        // do not support
    }

    @Override
    public boolean isDebugEnabled() {
        return log.isDebugEnabled();
    }

    @Override
    public void debug(String msg) {
        log.debug(msg);
    }

    @Override
    public void debug(String format, Object arg) {
        // do not support
    }

    @Override
    public void debug(String format, Object... arguments) {
        // do not support
    }

    @Override
    public void debug(String msg, Throwable t) {
        log.debug(msg, t);
    }

    @Override
    public boolean isDebugEnabled(Marker marker) {
        return false;
    }

    @Override
    public void debug(Marker marker, String msg) {
        // do not support
    }

    @Override
    public void debug(Marker marker, String format, Object arg) {
        // do not support
    }

    @Override
    public void debug(Marker marker, String format, Object... arguments) {
        // do not support
    }

    @Override
    public void debug(Marker marker, String msg, Throwable t) {
        // do not support
    }

    @Override
    public boolean isInfoEnabled() {
        return log.isInfoEnabled();
    }

    @Override
    public void info(String msg) {
        log.info(msg);
    }

    @Override
    public void info(String format, Object arg) {
        // do not support
    }

    @Override
    public void info(String format, Object... arguments) {
        // do not support
    }

    @Override
    public void info(String msg, Throwable t) {
        log.info(msg, t);
    }

    @Override
    public boolean isInfoEnabled(Marker marker) {
        return false;
    }

    @Override
    public void info(Marker marker, String msg) {
        // do not support
    }

    @Override
    public void info(Marker marker, String format, Object arg) {
        // do not support
    }

    @Override
    public void info(Marker marker, String format, Object... arguments) {
        // do not support
    }

    @Override
    public void info(Marker marker, String msg, Throwable t) {
        // do not support
    }

    @Override
    public boolean isWarnEnabled() {
        return false;
    }

    @Override
    public void warn(String msg) {
        log.warn(msg);
    }

    @Override
    public void warn(String format, Object arg) {
        // do not support
    }

    @Override
    public void warn(String format, Object... arguments) {
        // do not support
    }

    @Override
    public void warn(String msg, Throwable t) {
        log.warn(msg, t);
    }

    @Override
    public boolean isWarnEnabled(Marker marker) {
        return false;
    }

    @Override
    public void warn(Marker marker, String msg) {
        // do not support
    }

    @Override
    public void warn(Marker marker, String format, Object arg) {
        // do not support
    }

    @Override
    public void warn(Marker marker, String format, Object... arguments) {
        // do not support
    }

    @Override
    public void warn(Marker marker, String msg, Throwable t) {
        // do not support
    }

    @Override
    public boolean isErrorEnabled() {
        return false;
    }

    @Override
    public void error(String msg) {
        log.error(msg);
    }

    @Override
    public void error(String format, Object arg) {
        // do not support
    }

    @Override
    public void error(String format, Object... arguments) {
        // do not support
    }

    @Override
    public void error(String msg, Throwable t) {
        log.error(msg, t);
    }

    @Override
    public boolean isErrorEnabled(Marker marker) {
        return false;
    }

    @Override
    public void error(Marker marker, String msg) {
        // do not support
    }

    @Override
    public void error(Marker marker, String format, Object arg) {
        // do not support
    }

    @Override
    public void error(Marker marker, String format, Object... arguments) {
        // do not support
    }

    @Override
    public void error(Marker marker, String msg, Throwable t) {
        // do not support
    }
}

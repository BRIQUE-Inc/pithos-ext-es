package org.chronotics.pithos.ext.es.log;

public class LoggerFactory {
    private static ILoggerFactory loggerFactoryDelegate;

    static {
        String strClassName = "org.chronotics.pithos.ext.es.log.LogbackFactory";

        try {
            System.out.println("Defined Logger: " + strClassName);

            Class<?> loggerClass = Class.forName(strClassName.trim(), true, Thread.currentThread().getContextClassLoader());
            loggerFactoryDelegate = (ILoggerFactory) loggerClass.newInstance();
        }
        catch(Throwable e) {
            System.out.println(e.toString());
        }

        Logger logger = getLogger(LoggerFactory.class);
        logger.info("Using " + loggerFactoryDelegate + " for logging.");
    }

    public static final Logger getLogger(Class<?> clazz) {
        return loggerFactoryDelegate.createLogger(clazz);
    }

}

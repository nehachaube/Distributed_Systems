package system;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * CommonUtils contains miscellaneous static methods used by other classes.
 */
public class CommonUtils {

    /**
     * Initialize a logger. If any error occurs, exit the whole program.
     * 
     * @param loggerName
     *            the name of the logger
     * @param logfile
     *            the file to which log is written to
     * @param toConsole
     *            if <code>true</code>, write a copy of the log to the console
     * @return the initialized logger
     */
    public static Logger initializeLogger(String loggerName, String logfile, boolean toConsole) {
        Logger logger = null;
        try {
            logger = Logger.getLogger(loggerName);
            logger.setUseParentHandlers(false);

            Handler fileHandler = new FileHandler(logfile);
            fileHandler.setFormatter(new system.CustomizedFormatter());
            fileHandler.setLevel(Level.ALL);

            if (toConsole) {
                ConsoleHandler consoleHandler = new ConsoleHandler();
                consoleHandler.setFormatter(new CustomizedFormatter());
                consoleHandler.setLevel(Level.ALL);

                logger.addHandler(consoleHandler);
            }

            logger.addHandler(fileHandler);
        } catch (SecurityException | IOException e) {
            // the logger has not been initialized, so should not use logger.
            e.printStackTrace();
            System.exit(-1);
        }
        return logger;
    }

    /**
     * Send object to specified IP and port over UDP.
     * 
     * @param obj
     *            the object to send (must implements Serializable)
     * @param ip
     *            target ip address
     * @param port
     *            target port number
     * @param sendSocket
     *            use the socket to send
     * @throws IOException
     *             if any IO error occurs
     * 
     */
    public static void sendObjectOverUDP(Object obj, InetAddress ip, int port,
            DatagramSocket sendSocket) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(obj);
        byte[] bytes = baos.toByteArray();
        oos.close();

        DatagramPacket packet = new DatagramPacket(bytes, bytes.length, ip, port);
        sendSocket.send(packet);
    }
}

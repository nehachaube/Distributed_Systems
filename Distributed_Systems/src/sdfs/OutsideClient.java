package sdfs;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Scanner;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import system.Catalog;
import system.CustomizedFormatter;

public class OutsideClient extends Client {

    private final String nameNode;

    public OutsideClient(Logger logger, String nameNode) {
        this.nameNode = nameNode;
        this.logger = logger;
    }

    @Override
    protected Namenode getNamenode() throws RemoteException, NotBoundException {
        Registry registry = LocateRegistry.getRegistry(nameNode, Catalog.SDFS_NAMENODE_PORT);
        return (Namenode) registry.lookup("namenode");
    }

    public static void main(String[] args) {
        String nameNode = args[0];
        
        Logger logger = Logger.getLogger(OutsideClient.class.getName());
        logger.setUseParentHandlers(false);
        
        ConsoleHandler consoleHandler = new ConsoleHandler();
        consoleHandler.setFormatter(new CustomizedFormatter());
        consoleHandler.setLevel(Level.ALL);
        logger.addHandler(consoleHandler);
        
        Client client = new OutsideClient(logger, nameNode);
        
        Scanner in = new Scanner(System.in);
        String line;
        while ((line = in.nextLine()) != null) {
            try {
                if (line.startsWith("put")) {
                    String[] parts = line.split("\\s+");
                    String localFileName = parts[1];
                    String sdfsFileName = parts[2];
                    client.putFileOnSDFS(localFileName, sdfsFileName);
                } else if (line.startsWith("get")) {
                    String[] parts = line.split("\\s+");
                    String sdfsFileName = parts[1];
                    String localFileName = parts[2];
                    client.fetchFileFromSDFS(sdfsFileName, localFileName);
                } else if (line.startsWith("delete")) {
                    String[] parts = line.split("\\s+");
                    String sdfsFileName = parts[1];
                    client.deleteFileFromSDFS(sdfsFileName);
                } else if (line.startsWith("list")) {
                    String[] parts = line.split("\\s+");
                    String sdfsFileName = parts[1];
                    System.out.println(client.getFileLocations(sdfsFileName));
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, e.getMessage(), e);
            }
        }
        in.close();
    }
}

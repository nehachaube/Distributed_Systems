package sdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Client is an abstract class encapsulating basic client operations for SDFS.
 * Subclasses need to implement getNameNode() method, and assign it a Logger.
 */
public abstract class Client {

    protected Logger logger;

    protected abstract Namenode getNamenode() throws RemoteException, NotBoundException;

    public void putFileOnSDFS(String localFile, String sdfsFile)
            throws NotBoundException, IOException {
        logger.info(String.format("Start putting %s on SDFS %s.", localFile, sdfsFile));
        Namenode namenode = getNamenode();
        List<Datanode> datanodes = namenode.putRequest();
        Path filePath = Paths.get(localFile);
        byte[] fileContent = Files.readAllBytes(filePath);
        for (Datanode datanode : datanodes) {
            datanode.putFile(sdfsFile, fileContent);
        }
        logger.info(String.format("Successfully put %s on SDFS %s.", localFile, sdfsFile));
    }

    public void deleteFileFromSDFS(String file) throws NotBoundException, IOException {
        logger.info(String.format("Start deleting %s from SDFS.", file));
        Namenode namenode = getNamenode();
        namenode.deleteFile(file);
        logger.info(String.format("Successfully deleted %s from SDFS.", file));
    }

    public void fetchFileFromSDFS(String sdfsFile, String localFile)
            throws RemoteException, NotBoundException, FileNotFoundException {
        logger.info(String.format("Start getting %s from SDFS to %s.", sdfsFile, localFile));
        Namenode namenode = getNamenode();
        List<Datanode> datanodes = namenode.getFileLocations(sdfsFile);
        if (datanodes.isEmpty()) {
            throw new FileNotFoundException();
        }

        for (Datanode datanode : datanodes) {
            try {
                byte[] fileContent = datanode.getFile(sdfsFile);
                Path filePath = Paths.get(localFile);
                Files.write(filePath, fileContent);
                logger.info(
                        String.format("Successfully get %s from SDFS to %s.", sdfsFile, localFile));
                return;
            } catch (IOException e) {
                logger.log(Level.SEVERE, e.getMessage(), e);
            }
        }
        throw new RemoteException();
    }

    public List<InetAddress> getFileLocations(String file)
            throws RemoteException, NotBoundException {
        Namenode namenode = getNamenode();
        return namenode.getFileLocationIPs(file);
    }
}

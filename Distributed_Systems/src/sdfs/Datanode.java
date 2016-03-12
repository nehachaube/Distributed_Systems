package sdfs;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface Datanode extends Remote {
    void putFile(String fileName, byte[] fileContent) throws RemoteException, IOException;

    void deleteFile(String fileName) throws RemoteException, IOException;

    byte[] getFile(String fileName) throws RemoteException, IOException;

    void replicateFile(String fileName, Datanode... datanodes) throws RemoteException, IOException;
}

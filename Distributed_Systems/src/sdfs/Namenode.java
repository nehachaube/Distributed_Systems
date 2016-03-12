package sdfs;

import java.io.IOException;
import java.net.InetAddress;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface Namenode extends Remote {
    List<Datanode> getFileLocations(String fileName) throws RemoteException;

    List<InetAddress> getFileLocationIPs(String fileName) throws RemoteException;

    List<Datanode> putRequest() throws RemoteException;

    void deleteFile(String fileName) throws RemoteException, IOException;

    void updateMetadate(BlockReport blockReport) throws RemoteException;
    
    void addFile(String fileName, InetAddress address) throws RemoteException;
}

package sdfs;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.logging.Logger;

import system.Catalog;

public class InnerClient extends Client {
    private final LeaderElectionService les;
    
    public InnerClient(LeaderElectionService les, Logger logger) {
        this.les = les;
        this.logger = logger;
    }

    @Override
    protected Namenode getNamenode() throws RemoteException, NotBoundException {
        Registry registry = LocateRegistry.getRegistry(les.getLeader().IPAddress.getHostAddress(),
                Catalog.SDFS_NAMENODE_PORT);
        return (Namenode) registry.lookup("namenode");
    }
}

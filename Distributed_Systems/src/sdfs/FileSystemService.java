package sdfs;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Observable;
import java.util.Observer;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

import membershipservice.GossipGroupMembershipService;
import system.Catalog;
import system.CommonUtils;
import system.DaemonService;
import system.Identity;

public class FileSystemService implements DaemonService, Observer {

    private final DatanodeService dns;
    private NamenodeService nns;
    private final InnerClient client;
    private final GossipGroupMembershipService ggms;
    private final LeaderElectionService les;
    private final InetAddress selfIP;

    private final static Logger LOGGER = CommonUtils.initializeLogger(
            FileSystemService.class.getName(), Catalog.LOG_DIR + Catalog.SDFS_LOG, true);

    public FileSystemService() throws UnknownHostException {
        /*
         * if (System.getSecurityManager() == null) {
         * System.setSecurityManager(new SecurityManager()); }
         */
        selfIP = InetAddress.getLocalHost();

        ggms = new GossipGroupMembershipService(InetAddress.getByName(Catalog.INTRODUCER_ADDRESS),
                Catalog.SDFS_MEMBERSHIP_SERVICE_PORT, Catalog.SDFS_MEMBERSHIP_SERVICE_PORT);
        les = new LeaderElectionService(ggms, LOGGER);
        dns = new DatanodeService(les, LOGGER);
        les.addObserver(this);
        client = new InnerClient(les, LOGGER);
    }

    public GossipGroupMembershipService getMembershipService() {
        return this.ggms;
    }

    @Override
    public void startServe() throws IOException {
        nns = null;
        ggms.startServe();
        les.startServe();
        dns.startServe();
    }

    @Override
    public void stopServe() throws Exception {
        les.deleteObserver(this);
        if (nns != null) {
            nns.stopServe();
        }
        dns.stopServe();
        les.stopServe();
        ggms.stopServe();
    }

    @Override
    public void update(Observable o, Object arg) {
        Identity leader = (Identity) arg;
        if (leader.IPAddress.equals(selfIP)) {
            nns = new NamenodeService(ggms, LOGGER);
            try {
                nns.startServe();
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
                // "namenode can not start" is a fatal exception
                System.exit(-1);
            }
        }
    }

    public Client getClient() {
        return client;
    }

    public static void main(String[] args) throws Exception {

        FileSystemService fss = new FileSystemService();
        fss.startServe();
        Client client = fss.getClient();

        Scanner in = new Scanner(System.in);
        String line;
        while ((line = in.nextLine()) != null) {
            try {
                if (line.equals("Leave group")) {
                    fss.stopServe();
                    LOGGER.info("Current time.");
                } else if (line.equals("Join group")) {
                    fss.startServe();
                } else if (line.equals("Show membership list")) {
                    System.out.println(fss.ggms.getMembershipList());
                } else if (line.equals("Show self id")) {
                    System.out.println(fss.ggms.getSelfId());
                } else if (line.equals("Show leader")) {
                    System.out.println(fss.les.getLeader());
                } else if (line.equals("Show metadata")) {
                    if (fss.nns != null) {
                        System.out.println(fss.nns.getMetadata());
                    }
                } else if (line.startsWith("put")) {
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
                } else if (line.equals("store")) {
                    System.out.println(fss.dns.getSDFSFiles());
                } else if (line.startsWith("list")) {
                    String[] parts = line.split("\\s+");
                    String sdfsFileName = parts[1];
                    System.out.println(client.getFileLocations(sdfsFileName));
                }
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }
        in.close();
    }
}

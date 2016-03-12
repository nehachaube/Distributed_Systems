package membershipservice;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Observable;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import membershipservice.MembershipList.State;
import system.Catalog;
import system.CommonUtils;
import system.DaemonService;
import system.Identity;

/**
 * GossipGroupMembershipService is a daemon service, which implements gossip
 * membership protocol.
 */
public class GossipGroupMembershipService extends Observable implements DaemonService {

    private void notifyMemberChanges(List<MembershipList.MemberStateChange> mscList) {

        List<Identity> failedList = new ArrayList<>();
        for (MembershipList.MemberStateChange msc : mscList) {
            if (msc.toState == State.CLEANUP) {
                failedList.add(msc.id);
            }
        }
        if (!failedList.isEmpty()) {
            setChanged();
            notifyObservers(failedList);
        }
    }

    /**
     * GossipSender takes responsibility for gossiping its membership list to
     * other alive members.
     */
    private class GossipSender implements Runnable {
        @Override
        public void run() {
            try {
                Identity id = null;
                MembershipList nfm = null;
                // need to use synchronized here, otherwise membership list and
                // member state changes will be inconsistent
                synchronized (this) {
                    List<MembershipList.MemberStateChange> mscList = membershipList.update();
                    log(mscList);
                    notifyMemberChanges(mscList);
                    id = membershipList.getRandomAliveMember();
                    nfm = membershipList.getNonFailMembers();
                }
                if (id != null) {
                    synchronized (sendSocket) {
                        CommonUtils.sendObjectOverUDP(nfm, id.IPAddress, id.port, sendSocket);
                    }
                }
            } catch (IOException e) {
                // Exception means voluntarily leaving,
                // so it's safe to ignore it.
            }
        }
    }

    /**
     * IntroducerNegotiator is responsible for sending membership list to
     * introducer. Directly sending membership list to introducer is essential
     * for allowing introducer to function normally after restoring from crash.
     */
    private class IntroducerNegotiator implements Runnable {
        @Override
        public void run() {
            try {
                // It is ok to not update membership list here
                MembershipList nfm = membershipList.getNonFailMembers();
                synchronized (sendSocket) {
                    CommonUtils.sendObjectOverUDP(nfm, introducerIP, introducerPort, sendSocket);
                }
            } catch (IOException e) {
                // Exception means voluntarily leaving,
                // so it's safe to ignore it.
            }
        }
    }

    /**
     * GossipReceiver takes responsibility for receiving membership lists
     * gossiped from other members, and merging them with self local membership
     * list.
     */
    private class GossipReceiver implements Runnable {
        @Override
        public void run() {
            DatagramPacket packet = new DatagramPacket(new byte[Catalog.MAX_UDP_PACKET_BYTES],
                    Catalog.MAX_UDP_PACKET_BYTES);
            try {
                while (true) {
                    recSocket.receive(packet);
                    ByteArrayInputStream bais = new ByteArrayInputStream(packet.getData());
                    MembershipList receivedMsl = (MembershipList) new ObjectInputStream(bais)
                            .readObject();
                    synchronized (this) {
                        List<MembershipList.MemberStateChange> mscList = membershipList
                                .merge(receivedMsl);
                        log(mscList);
                        notifyMemberChanges(mscList);
                    }
                }
            } catch (IOException e) {
                // Exception means voluntarily leaving,
                // so it's safe to ignore it.
            } catch (ClassNotFoundException e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
                System.exit(-1);
            }
        }
    }

    private void log(List<MembershipList.MemberStateChange> mscList) {
        if (!mscList.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (MembershipList.MemberStateChange msc : mscList) {
                sb.append(msc + System.lineSeparator());
            }
            sb.delete(sb.length() - System.lineSeparator().length(), sb.length());

            LOGGER.info(sb.toString());
            LOGGER.info(String.format("Current membership list is:%n%s", membershipList));
        }
    }

    private final InetAddress introducerIP;
    private final int introducerPort;
    private final int selfPort;
    private MembershipList membershipList;
    private DatagramSocket sendSocket;
    private DatagramSocket recSocket;
    private ScheduledExecutorService scheduler;
    // java Logger is thread safe
    private final static Logger LOGGER = CommonUtils.initializeLogger(
            GossipGroupMembershipService.class.getName(),
            Catalog.LOG_DIR + Catalog.MEMBERSHIP_SERVICE_LOG, false);

    public GossipGroupMembershipService(InetAddress introducerIP, int introducerPort,
            int selfPort) {
        this.introducerIP = introducerIP;
        this.introducerPort = introducerPort;
        this.selfPort = selfPort;
    }

    @Override
    public void startServe() throws IOException {
        recSocket = new DatagramSocket(selfPort);
        sendSocket = new DatagramSocket();
        membershipList = new MembershipList(
                new Identity(InetAddress.getLocalHost(), selfPort, System.currentTimeMillis()));
        scheduler = Executors.newScheduledThreadPool(2);

        scheduler.scheduleAtFixedRate(new GossipSender(), 0, Catalog.GOSSIP_GAP, Catalog.TIME_UNIT);
        if (!introducerIP.equals(InetAddress.getLocalHost()) || introducerPort != selfPort) {
            scheduler.scheduleAtFixedRate(new IntroducerNegotiator(), 0,
                    Catalog.INTRODUCER_NEGOTIATE_GAP, Catalog.TIME_UNIT);
        }
        new Thread(new GossipReceiver()).start();
    }

    @Override
    public void stopServe() {
        scheduler.shutdown();
        // Wait until all tasks finish
        while (!scheduler.isTerminated()) {
        }
        recSocket.close();

        LOGGER.info("LEAVE: " + getSelfId());
        MembershipList vlm = membershipList.voluntaryLeaveMessage();
        try {
            for (int i = 0; i < Catalog.NUM_LEAVE_GOSSIP; i++) {
                Identity id = membershipList.getRandomAliveMember();
                if (id != null) {
                    synchronized (sendSocket) {
                        CommonUtils.sendObjectOverUDP(vlm, id.IPAddress, id.port, sendSocket);
                    }
                }
            }
        } catch (IOException e) {
            // IOExceptin here means real IOException
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
            System.exit(-1);
        }
        sendSocket.close();
    }

    /**
     * @return a list of alive members in the group including self.
     */
    public List<Identity> getAliveMembers() {
        return membershipList.getAliveMembersIncludingSelf();
    }

    /**
     * @return a list of alive members in the group excluding self.
     */
    public List<Identity> getAliveMembersExcludeSelf() {
        return membershipList.getAliveMembersExceptSelf();
    }

    /**
     * @return the oldest alive member in the group including self.
     */
    public Identity getOldestAliveMember() {
        return membershipList.getOldestAliveMember();
    }

    /**
     * @return the membership list
     */
    public MembershipList getMembershipList() {
        return membershipList;
    }

    /**
     * @return self's id in the group
     */
    public Identity getSelfId() {
        return membershipList.getSelfId();
    }

    public static void main(String[] args) throws IOException {
        String introducer = args[0];
        int introducerPort = Integer.parseInt(args[1]);
        int selfPort = Integer.parseInt(args[2]);

        GossipGroupMembershipService ggms = new GossipGroupMembershipService(
                InetAddress.getByName(introducer), introducerPort, selfPort);
        ggms.startServe();

        Scanner in = new Scanner(System.in);
        String line;
        while ((line = in.nextLine()) != null) {
            if (line.equals("Leave group")) {
                ggms.stopServe();
            } else if (line.equals("Join group")) {
                ggms.startServe();
            } else if (line.equals("Show membership list")) {
                System.out.println(ggms.getMembershipList());
            } else if (line.equals("Show self id")) {
                System.out.println(ggms.getSelfId());
            }
        }
        in.close();
    }
}

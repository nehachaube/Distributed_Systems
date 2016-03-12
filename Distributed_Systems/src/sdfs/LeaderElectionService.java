package sdfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Observable;
import java.util.Observer;
import java.util.logging.Level;
import java.util.logging.Logger;

import membershipservice.GossipGroupMembershipService;
import system.Catalog;
import system.DaemonService;
import system.Identity;

/**
 * LeaderElectionService is built on top of GossipGroupMembershipService. When
 * Starting a LeaderElectionService, the underlying GossipGroupMembershipService
 * must already have started. And stopServe should be in reverse order, i.e.,
 * close LeaderElectionService first, then GossipGroupMembershipService.
 */
public class LeaderElectionService extends Observable implements DaemonService, Observer {

    private final GossipGroupMembershipService ggms;
    private Identity leader;
    private final Logger logger;

    public LeaderElectionService(GossipGroupMembershipService ggms, Logger logger) {
        this.ggms = ggms;
        this.logger = logger;
        this.leader = null;
    }

    // This method needs to be synchronized. Very trick!
    private synchronized void setLeader(Identity leader) {
        logger.info(String.format("Elect %s as the new leader.", leader));
        this.leader = leader;
        setChanged();
        notifyObservers(leader);
    }

    // This method needs to be synchronized.
    public synchronized Identity getLeader() {
        return leader;
    }

    // This method needs to be synchronized.
    @Override
    public synchronized void addObserver(Observer o) {
        if (this.leader != null) {
            o.update(this, this.leader);
        }
        super.addObserver(o);
    }

    @Override
    public void startServe() throws IOException {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    // Wait for a while for membership list to get stable
                    Thread.sleep(Catalog.MEMBER_JOIN_TIME);
                    // Very tricky here!
                    // When setting the leader, we need to ensure membership
                    // list of group membership service does not change.
                    synchronized (ggms) {
                        setLeader(ggms.getOldestAliveMember());
                        ggms.addObserver(LeaderElectionService.this);
                    }
                } catch (InterruptedException e) {
                    logger.log(Level.SEVERE, e.getMessage(), e);
                }
            }
        }).start();
    }

    @Override
    public void stopServe() {
        // synchronized ggms.getMembershipList() to ensure update() will not be
        // called once more.
        ggms.deleteObserver(this);
        this.leader = null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void update(Observable o, Object arg) {
        // For simplicity, assume we will always elect the eldest member as the
        // leader, i.e., it will not be detected
        // as failure when at first election.
        List<Identity> failedNodes = (ArrayList<Identity>) arg;
        if (failedNodes.contains(leader)) {
            Identity oldestMember = ggms.getOldestAliveMember();
            logger.info(String.format("Old leader %s failed.", leader));
            setLeader(oldestMember);
        }
    }
}

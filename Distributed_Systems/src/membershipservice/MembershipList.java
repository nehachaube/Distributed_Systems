package membershipservice;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import system.Catalog;
import system.Identity;

/**
 * MembershipList is a thread safe class which implements membership list used
 * by group membership protocol. It represents a local view of the group from a
 * member. It is internally implemented using sorted ArrayList, so as to support
 * quick merge and quick random choice. Besides using synchronized for every
 * method, the Member class need to be immutable in order to void race
 * conditions. Think about it carefully!
 */
public class MembershipList implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * There are 4 kinds of states for a member: fail, alive, leave, and delete.
     * Delete will actually never appear, it is only used to represent the event
     * a member is deleted from membership list.
     */
    public static enum State {
        FAIL, ALIVE, LEAVE, CLEANUP,
    }

    /**
     * MemberStateChange represents state change of a member.
     */
    public static class MemberStateChange {
        Identity id;
        State toState;

        MemberStateChange(Identity id, State toState) {
            this.id = id;
            this.toState = toState;
        }

        @Override
        public String toString() {

            switch (toState) {
            case LEAVE:
                return "LEAVE: " + id.toString();
            case FAIL:
                return "FAIL: " + id.toString();
            case ALIVE:
                return "JOIN: " + id.toString();
            case CLEANUP:
                return "CLEANUP: " + id.toString();
            default:
                // should never reach here
                return null;
            }
        }
    }

    /**
     * Member is an immutable class that stores information about a member in
     * membership list. Think about it carefully! If Member is allowed to
     * change, then it will introduce race conditions!
     */
    public static class Member implements Serializable {
        private static final long serialVersionUID = 1L;

        final Identity id;
        final int heartbeatCounter;
        final transient long lastUpdateTime;
        final State state;

        public static final Comparator<Member> compareByIdentity = new Comparator<Member>() {
            @Override
            public int compare(Member o1, Member o2) {
                return o1.id.compareTo(o2.id);
            }
        };

        Member(Identity id, int heartbeatCounter, long lastUpdateTime, State state) {
            this.id = id;
            this.heartbeatCounter = heartbeatCounter;
            this.lastUpdateTime = lastUpdateTime;
            this.state = state;
        }
    }

    private List<Member> membershipList;
    private transient final Identity selfId;

    /**
     * Construct membership list with self id. It contains self as initial
     * member.
     * 
     * @param selfId
     *            the identity of self
     */
    public MembershipList(Identity selfId) {
        membershipList = new ArrayList<>();
        membershipList.add(new Member(selfId, 0, System.currentTimeMillis(), State.ALIVE));
        this.selfId = selfId;
    }

    private MembershipList() {
        membershipList = new ArrayList<>();
        this.selfId = null;
    }

    /**
     * @return self's id. This method does not need to be synchronized, because
     *         selfId is immutable.
     */
    public Identity getSelfId() {
        return selfId;
    }

    /**
     * Merge with another membership list. Add new members, and update states
     * for old members. If another membership list containing a LEAVE member,
     * which is not contained in the membership list, then just ignore it.
     * 
     * @param that
     *            the membership list to merge with
     * @return a list containing state changes for members
     */
    public synchronized List<MemberStateChange> merge(MembershipList that) {
        long currentTime = System.currentTimeMillis();

        List<MemberStateChange> mscList = new ArrayList<>();

        int i = 0;
        int j = 0;
        List<Member> newml = new ArrayList<>();
        while (i < size() && j < that.size()) {
            Member m1 = membershipList.get(i);
            Member m2 = that.membershipList.get(j);

            int cp = Member.compareByIdentity.compare(m1, m2);
            if (cp == 0) {
                i++;
                j++;
                if (m2.heartbeatCounter > m1.heartbeatCounter) {
                    if (m1.state != m2.state) {
                        mscList.add(new MemberStateChange(m1.id, m2.state));
                    }
                    newml.add(new Member(m1.id, m2.heartbeatCounter, currentTime, m2.state));
                } else {
                    newml.add(m1);
                }
            } else if (cp < 0) {
                i++;
                newml.add(m1);
            } else {
                newml.add(new Member(m2.id, m2.heartbeatCounter, currentTime, m2.state));
                mscList.add(new MemberStateChange(m2.id, m2.state));
                j++;
            }
        }

        while (i < size()) {
            newml.add(membershipList.get(i++));
        }

        while (j < that.size()) {
            Member m = that.membershipList.get(j++);
            // if state of the member is LEAVE, and it is not in my membership
            // list, then just ignore it, otherwise there is high probability
            // to cause LEAVE message to last forever.
            if (m.state == State.ALIVE) {
                newml.add(new Member(m.id, m.heartbeatCounter, currentTime, m.state));
                mscList.add(new MemberStateChange(m.id, m.state));
            }
        }

        membershipList = newml;
        return mscList;
    }

    /**
     * @return the identity of all the alive members except self
     */
    public synchronized List<Identity> getAliveMembersExceptSelf() {
        List<Identity> list = new ArrayList<>();
        for (Member m : membershipList) {
            if (!m.id.equals(selfId) && m.state == State.ALIVE) {
                list.add(m.id);
            }
        }
        return list;
    }

    public synchronized List<Identity> getAliveMembersIncludingSelf() {
        List<Identity> list = new ArrayList<>();
        for (Member m : membershipList) {
            if (m.state == State.ALIVE) {
                list.add(m.id);
            }
        }
        return list;
    }

    public synchronized Identity getOldestAliveMember() {
        List<Identity> members= getAliveMembersIncludingSelf();
        Identity oldestMember = Collections.min(members, new Comparator<Identity>() {
            @Override
            public int compare(Identity o1, Identity o2) {
                long t = o1.timestamp - o2.timestamp;
                return t < 0 ? -1 : t == 0 ? 0 : 1;
            }
        });
        return oldestMember;
    }
    
    /**
     * Increment self heart beat counter, and clean up failed members.
     * 
     * @return a list containing state changes for members
     */
    public synchronized List<MemberStateChange> update() {
        long currentTime = System.currentTimeMillis();

        List<Member> newml = new ArrayList<>();
        List<MemberStateChange> mscList = new ArrayList<>();

        for (Member m : membershipList) {
            if (m.id.equals(selfId)) {
                newml.add(new Member(m.id, m.heartbeatCounter + 1, currentTime, State.ALIVE));
            } else {
                long t = currentTime - m.lastUpdateTime;
                if (t <= Catalog.CLEANUP_TIME) {
                    if (t > Catalog.FAIL_TIME && m.state == State.ALIVE) {
                        newml.add(
                                new Member(m.id, m.heartbeatCounter, m.lastUpdateTime, State.FAIL));
                        mscList.add(new MemberStateChange(m.id, State.FAIL));
                    } else {
                        newml.add(m);
                    }
                } else {
                    mscList.add(new MemberStateChange(m.id, State.CLEANUP));
                }
            }
        }

        membershipList = newml;
        return mscList;
    }

    /**
     * @return a MembershipList containing all the non fail members, including
     *         self and leave nodes (as long as they have not pass the
     *         FAIL_TIME, so the leave message can be gossiped).
     */
    public synchronized MembershipList getNonFailMembers() {
        MembershipList ml = new MembershipList();
        long currentTime = System.currentTimeMillis();

        for (Member m : membershipList) {
            if (m.state != State.FAIL && !(m.state == State.LEAVE
                    && currentTime - m.lastUpdateTime > Catalog.FAIL_TIME)) {
                ml.membershipList.add(m);
            }

        }
        return ml;
    }

    /**
     * @return the identity of a random alive member except self. If no such
     *         member exists, return <code>null</code>
     */
    public synchronized Identity getRandomAliveMember() {
        List<Identity> aliveMembers = getAliveMembersExceptSelf();
        if (aliveMembers.size() == 0) {
            return null;
        } else {
            int k = ThreadLocalRandom.current().nextInt(aliveMembers.size());
            return aliveMembers.get(k);
        }
    }

    /**
     * @return the MembershipList only containing self with an incrementing
     *         heart beat counter and leave state
     */
    public synchronized MembershipList voluntaryLeaveMessage() {
        int idx = Collections.binarySearch(membershipList, new Member(selfId, 0, 0, State.ALIVE),
                Member.compareByIdentity);
        Member self = membershipList.get(idx);
        MembershipList vlm = new MembershipList();
        // Add a large number to heartbeatCounter to avoid race conditions with
        // other Gossip threads.
        vlm.membershipList.add(new Member(self.id, self.heartbeatCounter + 10, 0, State.LEAVE));
        return vlm;
    }

    /**
     * @return the number of members in the membership list.
     */
    public synchronized int size() {
        return membershipList.size();
    }

    @Override
    public synchronized String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("[%n"));
        for (Member m : membershipList) {
            sb.append(String.format("\t%s\t%s\t%s\t%s%n", m.id, m.heartbeatCounter,
                    m.lastUpdateTime, m.state));
        }
        sb.append("]");
        return sb.toString();
    }
}

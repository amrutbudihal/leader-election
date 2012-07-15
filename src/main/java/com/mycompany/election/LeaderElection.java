package com.mycompany.election;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;


public class LeaderElection implements Watcher {

    private ZooKeeper zk;
    private final int TIMEOUT=5000;
    private CountDownLatch latch = new CountDownLatch(1);
    private static final String ELECTION="/ELECTION";
    private static final String NODE="/NODE-";
    private String nodePath;

    public ZooKeeper getZk() {
        return zk;
    }

    public void setZk(ZooKeeper zk) {
        this.zk = zk;
    }

    public void setNodePath(String nodePath) {
        this.nodePath = nodePath;
    }

    public LeaderElection() throws IOException, InterruptedException, KeeperException {
        
        setZk(new ZooKeeper("localhost", TIMEOUT, this));
        
        Stat s = getZk().exists(ELECTION, this);
        if (s == null) {
            getZk().create(ELECTION, "This node is used for election.".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        //wait till SyncConnected Signal is received in process method.
        latch.await();

        setNodePath(getZk().create(ELECTION + NODE, null /*no data yet*/,
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL));

        elect();

    }

    // If leader is deleted, the immediate follower becomes leader.
    // If in between follower is deleted, link is broken, call this method again to re-establish that link.
    private void elect() throws IOException, InterruptedException, KeeperException {
        List<String> nodes =  getZk().getChildren(ELECTION, new ElectionNodeWatcher());

        Collections.sort(nodes); //sort guarantees lowest member at the top.

        int length = nodes.size();
        for(int i=0; i<length; i++) {
            String nodePath = ELECTION+"/"+nodes.get(i);
            if (nodePath.equals(this.nodePath)) {
                //set watch on the previous node.
                if (i == 0) {
                    //the first node and hence elect yourself as a leader.
                    System.out.println("Leader:"+nodePath);
                    getZk().setData(nodePath, "Yay, Me KING, Me RULE! I AM THE LEADER".getBytes(), -1);
                } else {
                    System.out.println("Follower:"+nodePath);
                    getZk().setData(nodePath, "Booo, I AM FOLLOWER!".getBytes(), -1);
                    //avoid herd effect, set watch only on previous node.
                    String previousNodePath = ELECTION+"/"+nodes.get(i - 1);
                    System.out.println("Following:"+previousNodePath);
                    getZk().exists(previousNodePath, new IndividualNodeWatcher());
                }
            }
        }  
    }

    public void process(WatchedEvent event) {
            //initial state.
            if(event.getState() == Event.KeeperState.SyncConnected) {
                latch.countDown();
            }
    }

    private class ElectionNodeWatcher implements Watcher {
        //used to watch /ELECTION node's children changes.
        public void process(WatchedEvent event) {

            if(event.getType() == Event.EventType.NodeChildrenChanged) {
                System.out.println("Node changed event, RE-ELECTION.");
                try {
                    elect();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private class IndividualNodeWatcher implements Watcher {
        //used to watch /ELECTION/NODE- deleted changes.
        public void process(WatchedEvent event) {
            if(event.getType() == Event.EventType.NodeDeleted) {
                System.out.println("Node delete event, RE-ELECTION.");
                try {
                    elect();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        new LeaderElection();
        Thread.sleep(180000);
    }
}

/* 
 * Distributed Rise And Fall spatio-Temporal (DRAFT) clustering algorithm for the ONE simulator
 * Copyright 2012 Matthew Orlinski
 * Released under GPLv3. See LICENSE.txt for details. 
 */

package routing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import javax.swing.tree.DefaultMutableTreeNode;

import core.Connection;
import core.DTNHost;
import core.Message;
import core.Settings;
import core.SimClock;
import core.Tuple;

public class DRAFT extends ActiveRouter {

	public static final String DRAFT_NS = "DRAFT";
	public static int ran2 = 0, ran3 = 0;
	/**
	 * Total contact time threshold for adding a node to the familiar set
	 * -setting id {@value}
	 */
	/*
	 * # ##EXAMPLE## DRAFT protocol settings Group.router = DRAFT
	 * DRAFT.familiarThreshold = 1000 DRAFT.degrade = 0.7 DRAFT.frameSize = 300
	 * 下面几个变量与脚本中的这个几个参数对应
	 */
	public static final String FAMILIAR_SETTING = "familiarThreshold";
	public static final String DEGRADE = "degrade";
	public static final String FRAME_SIZE = "frameSize";

	// current working variables
	public Map<DTNHost, Double> neighbourSet;
	public Set<DTNHost> markedForDeletion;
	public Map<DTNHost, DefaultMutableTreeNode> localClusterCache;
	public Map<DTNHost, Double> meetSet;// 存储与目的节点相遇次数

	protected double familiarThreshold;
	protected double degrade;
	protected int frameSize;

	public DRAFT(Settings s) {
		super(s);
		Settings simpleSettings = new Settings(DRAFT_NS);
		this.familiarThreshold = simpleSettings.getDouble(FAMILIAR_SETTING);
		this.degrade = simpleSettings.getDouble(DEGRADE);
		this.frameSize = simpleSettings.getInt(FRAME_SIZE);

	}

	public DRAFT(DRAFT proto) {
		super(proto);
		this.frameSize = proto.frameSize;
		this.familiarThreshold = proto.familiarThreshold;
		this.degrade = proto.degrade;

		neighbourSet = new HashMap<DTNHost, Double>();
		markedForDeletion = new HashSet<DTNHost>();
		localClusterCache = new HashMap<DTNHost, DefaultMutableTreeNode>();
		meetSet = new HashMap<DTNHost, Double>();

	}

	@Override
	public DRAFT replicate() {
		return new DRAFT(this);
	}

	@Override
	public void changedConnection(Connection con) {
		DTNHost myHost = getHost(); // 获得draft路由协议所在节点
		DTNHost otherNode = con.getOtherNode(myHost); // 获得con连接的另一节点
		DRAFT otherRouter = (DRAFT) otherNode.getRouter(); // 获得otherNode节点上运行的路由协议

		if (con.isUp()) // 连接的状态为up
		{
			// 如果相遇节点在neighbourSet已有则更新其value为＋1，否则新纪录插入
			if (this.neighbourSet.containsKey(otherNode)) {
				this.neighbourSet.put(otherNode, this.neighbourSet.get(otherNode) + 1);
				// this.FCluster.put();
			} else
				this.neighbourSet.put(otherNode, 1.0);

			// check local community information with new connections
			// 达到要求则加入到本地通信组
			checkLocalCommunity(con);

			// Do node deletion
			// 如果两节点属于同一簇，两节点删除队列中共有的节点将立即从簇中移除，并在删除队列中移除peer节点的邻居节点
			if (this.localClusterCache.containsKey(otherNode)) {
				Iterator<DTNHost> it = this.markedForDeletion.iterator();
				while (it.hasNext()) {
					DTNHost host = it.next();

					if (otherRouter.markedForDeletion.contains(host)) {
						it.remove();
						this.localClusterCache.remove(host);
						otherRouter.markedForDeletion.remove(host);
						otherRouter.localClusterCache.remove(host);
					}
				}

				Iterator<DTNHost> its = this.markedForDeletion.iterator();
				while (its.hasNext()) {
					DTNHost host = its.next();
					if (otherRouter.neighbourSet.containsKey(host)) {
						its.remove();
					}
				}
			}
		}
	}

	// 达到要求则加入到本地通信组localClusterCache
	public void checkLocalCommunity(Connection con) {

		// 获取当前连接的另外一端的节点，并将其协议强制转换为DRAFT
		DTNHost peer = con.getOtherNode(getHost());
		DRAFT peerC = (DRAFT) con.getOtherNode(getHost()).getRouter();

		// 2) check that the connection has met the time threshold. If so:
		if (this.neighbourSet.get(peer) >= this.familiarThreshold) {// 判断是否大于阈值，如果是则
			// System.out.println("Peer " + peer + " passed familiar
			// threshold.");
			// 3) if device is in Do then remove it如果peer存在于Do中，则把它移除；
			if (this.markedForDeletion.contains(peer))
				this.markedForDeletion.remove(peer);

			// 4.1 if the node is still not in the local community, give it
			// another chance with this secondary promotion mechanism
			if (!this.localClusterCache.containsKey(peer)) {
				// 把节点peer及它的簇树都加入进来
				// 这样做的话节点链接的簇树会有重复的节点，作用是什么？保证peer节点还有机会再次加入localcluster
				this.localClusterCache.put(peer, peerC.getLocalCluster(this.getHost()));
			}
		}
	}

	// This function uses some tree logic when there is no need
	// (sorry it was the result of a copy/paste)
	// 获得本节点为根不包含dtnost的簇树
	public DefaultMutableTreeNode getLocalCluster(DTNHost dtnHost) {

		DefaultMutableTreeNode root = new DefaultMutableTreeNode(this.getHost());
		Iterator<Entry<DTNHost, DefaultMutableTreeNode>> it = this.localClusterCache.entrySet().iterator();
		while (it.hasNext()) {
			Entry<DTNHost, DefaultMutableTreeNode> pairs = it.next();
			DTNHost host = (DTNHost) pairs.getKey();
			if (host != dtnHost && host != this.getHost()) {
				DefaultMutableTreeNode child = new DefaultMutableTreeNode(host);
				child.add(pairs.getValue());
				root.add(child);
			}
		}
		return root;
	}

	protected boolean commumesWithHost(DTNHost h) {
		return (this.localClusterCache.containsKey(h));
	}

	// 对接触节点发送消息，只发送目的节点与接触节点处于同一簇的消息
	private Tuple<Message, Connection> tryOtherMessages() {

		List<Tuple<Message, Connection>> messages = new ArrayList<Tuple<Message, Connection>>();
		ArrayList<Tuple<Message, Connection>> waitConList = new ArrayList<Tuple<Message, Connection>>();// 等待队列
		Collection<Message> msgCollection = getMessageCollection();

		for (Connection con : getConnections()) {// 对所有连接，也即是对接触节点发送消息
			DTNHost other = con.getOtherNode(getHost());
			DRAFT othRouter = (DRAFT) other.getRouter();

			if (othRouter.isTransferring()) { // 对方忙则忽略
				continue; // skip hosts that are transferring
			}

			// 一个节点携带的message很多，每个message的目的节点不一样
			Double a = 0.0;

			for (Message m : msgCollection) {
				if (othRouter.hasMessage(m.getId())) {// 对方已经收到过了则忽略
					continue; // skip messages that the other one has
				}

				// 我的想法：如果接触节点与目的节点相遇次数多（相当于亲密度--该算法中利用相遇次数来当作累计相遇时间，不太合理，加入我以前做的获取时间的工作可能会好一点），则也进入消息队列（未完成）
				/*
				 * 前提：接触节点与目的节点相遇过
				 * 优先级：接触节点与目的节点同簇>接触节点在LocalCluster中>接触节点在NeighborSet中
				 * 做法：新建一个cluster，用来记接触节点与目的节点相遇的次数（Map<DTNHost, Double>
				 * meetSet;）
				 */
				// 我的想法（失败，不可行，本方法就是LocalCluster中进行的）：如果接触节点neighborSet或者LocalCluster中包含有与目的节点相遇过的（相当于共同好友个数），将该节点加入消息队列

				// 如果目的节点与对方是同簇才会进入消息队列（等级最高，最优先加入消息队列）

				if (othRouter.meetSet.get(m.getTo()) != null&&othRouter.commumesWithHost(m.getTo())) {
					if (othRouter.meetSet.get(m.getTo()) >= 100)// 如果接触节点与目的节点相遇次数大于10，进入消息队列；（等级第二）
					{
						ran2++;
						System.out.println("等级二：" + othRouter.meetSet.get(m.getTo()) + "+" + ran2);
						messages.add(new Tuple<Message, Connection>(m, con));
						// continue;
					} else if (othRouter.meetSet.get(m.getTo()) > 20) // 如果接触节点与目的节点相遇次数小于10，大于0，则等待下一个，直到遇见等级比他低的（与目的节点相遇次数为0或者小于该节点的次数）为止（等级第三）
					{
						ran3++;
						System.out.println("等级三：" + othRouter.meetSet.get(m.getTo()) + "+" + ran3);
						// 做法猜想：建立一个栈，将该节点加入栈中，每次来了新节点就进行比较， //
						// 如果新节点级数较低，则该节点出栈，加入消息队列，新节点入栈； //
						// 否则，若相等，则新节点和老节点加入消息队列；如果新节点级数较高，则直接加入消息队列，栈不变。

						if (waitConList.isEmpty()) {// 如果栈为空，则将该节点入栈，等待下一节点进行比较。
							a = othRouter.meetSet.get(m.getTo());
							waitConList.add(new Tuple<Message, Connection>(m, con));
							// messages.add(new Tuple<Message,
							// Connection>(m,con));
							continue;
						} else {
							if (othRouter.meetSet.get(m.getTo()) < a) {// 如果新节点级数较低，则该节点出栈，加入消息队列，新节点入栈；
								messages.add(waitConList.get(0));
								waitConList.remove(0);
								waitConList.add(new Tuple<Message, Connection>(m, con));
								a = othRouter.meetSet.get(m.getTo());
								continue;
							} else if (othRouter.meetSet.get(m.getTo()) == a) {// 若相等，则新节点和老节点加入消息队列,清空栈；
								messages.add(waitConList.get(0));
								messages.add(new Tuple<Message, Connection>(m, con));
								waitConList.remove(0);
								continue;
							} else {// 如果新节点级数较高，则直接加入消息队列，栈不变。
								messages.add(new Tuple<Message, Connection>(m, con));
								continue;
							}
						}
					}
				}

			}
		}
		if (!waitConList.isEmpty()) {
			messages.add(waitConList.get(0));
			waitConList.remove(0);
		}
		if (messages.size() == 0) {
			return null;
		}
		Collections.sort(messages,new Comparator<Tuple<Message, Connection>>(){

			@Override
			public int compare(Tuple<Message, Connection> m,
					Tuple<Message, Connection> n) {
				
				DRAFT mRouter=(DRAFT)m.getValue().getOtherNode(getHost()).getRouter();			
				DRAFT nRouter=(DRAFT)n.getValue().getOtherNode(getHost()).getRouter();
				if(mRouter.meetSet.get(m.getKey())!=null&&nRouter.meetSet.get(m.getKey())!=null){
					Double mm=mRouter.meetSet.get(m.getKey());
					Double nn=mRouter.meetSet.get(n.getKey());
					return mm.compareTo(nn);
				}else
					return 0;
			}
			
		});
		// sort the message-connection tuples 发送消息，了解消息发送机制可继续跟踪
		return tryMessagesForConnected(messages); // try to send messages
	}

	// update是路由协议的重要方法，执行向接触节点发送消息，同时更新neighbourSet、簇和节点累计值
	//
	@Override
	public void update() {
		super.update();
		if (isTransferring() || !canStartTransfer()) {
			return;
		}

		if (exchangeDeliverableMessages() != null) {
			return;
		}
		tryOtherMessages();// 对接触节点发送消息，只发送目的节点与接触节点处于同一簇的消息（如果以前相遇过呢？）

		// 我的想法：不一定非要是同一簇，可以划分等级。比如目的节点与接触节点处于同一簇的，优先级最高；如果接触节点

		Collection<Message> msgCollection = getMessageCollection();

		// For each connection increment the connection time by 1
		for (Connection c : getConnections()) {
			DTNHost peer = c.getOtherNode(getHost());// 获取当前连接的另外一端的节点；
			DRAFT peerC = (DRAFT) c.getOtherNode(getHost()).getRouter();

			// 初始化neighbourSet
			/*
			 * 我的想法：如果接触时间超过某一个值，则将peer次数加1；否则视为没有相遇；（如何获取相遇时间？）已验证：
			 * 改变衰减率的值对结果影响效果不明显
			 */
			// if(c.getConnectiontime()>=100){
			if (this.neighbourSet.containsKey(peer)) {// 如果neighbourSet包含当前连接的节点peer，则
				this.neighbourSet.put(peer, this.neighbourSet.get(peer) + 1);// 将peer次数加一；
			} else {
				this.neighbourSet.put(peer, 1.0);// 不包含则将peer次数设为1；
			}

			// 在这统计与目的节点相遇次数
			for (Message m : msgCollection) {
				if (peer == m.getTo()) {// 接触节点是目的节点，则将meet次数加一
					System.out.println("###########3");
					this.localClusterCache.put(peer, peerC.getLocalCluster(this.getHost()));
					if (this.meetSet.containsKey(peer)) {
						this.meetSet.put(peer, this.meetSet.get(peer) + 1);
						System.out.println("###########2");
					} else {
						this.meetSet.put(peer, 1.0);
						System.out.println("###########1");
					}
				}
			}

			if (this.neighbourSet.get(peer) >= this.familiarThreshold) {// 如果次数大于阈值，则
				checkLocalCommunity(c);// 达到要求则加入到本地通信组localClusterCache
			}

		}

		double simTime = SimClock.getTime(); // (seconds since start)

		double timeInFrame = simTime % this.frameSize;

		// 在每一个time frame开始的时候，对簇进行更新，剔除掉删除队列中的节点，对所有节点累计值进行衰减
		if (timeInFrame == 0) {
			emptyMarkedForDeletion();
			decreaseAllNodes();
		}

	}

	public void markNodeForDeletion(DTNHost node) {
		this.markedForDeletion.add(node);
	}

	// 节点累计值的衰减
	private void decreaseAllNodes() {
		Iterator<Entry<DTNHost, Double>> it = neighbourSet.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pairs = it.next();
			Double newValue = (((Double) pairs.getValue()) * this.degrade);
			neighbourSet.put((DTNHost) pairs.getKey(), newValue);
			// 簇中节点的累计值如果低于阈值则进入删除队列
			if (newValue < (this.familiarThreshold) && this.localClusterCache.containsKey((DTNHost) pairs.getKey())) {
				markNodeForDeletion((DTNHost) pairs.getKey());
			}
		}
	}

	/**
	 * empties marked for deletion
	 */
	// 把delete队列中的节点从簇中移除,delete队列什么时候清空呢？？--每个time frame开始时
	public void emptyMarkedForDeletion() {
		// Any nodes that are still in the mark for deletion pile, delete
		for (DTNHost host : this.markedForDeletion) {
			if (this.localClusterCache.containsKey(host)) {
				this.localClusterCache.remove(host);
			}
		}
	}

}

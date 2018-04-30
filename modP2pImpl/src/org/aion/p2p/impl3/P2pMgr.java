/*
 * Copyright (c) 2017-2018 Aion foundation.
 *
 * This file is part of the aion network project.
 *
 * The aion network project is free software: you can redistribute it
 * and/or modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation, either version 3 of
 * the License, or any later version.
 *
 * The aion network project is distributed in the hope that it will
 * be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with the aion network project source files.
 * If not, see <https:www.gnu.org/licenses/>.
 *
 * Contributors to the aion source files in decreasing order of code volume:
 *
 * Aion foundation.
 *
 */

package org.aion.p2p.impl3;

import org.aion.p2p.*;
//import org.aion.p2p.impl.TaskUPnPManager;
import org.aion.p2p.impl.comm.Act;
import org.aion.p2p.impl.zero.msg.*;
//import org.apache.commons.collections4.map.LRUMap;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author chris
 */
public final class P2pMgr implements IP2pMgr {

    private final static int PERIOD_SHOW_STATUS = 10000;
    private final static int PERIOD_REQUEST_ACTIVE_NODES = 1000;
    private final static int PERIOD_CONNECT_OUTBOUND = 1000;
    private final static int PERIOD_CLEAR = 10000;
    private final static int PERIOD_UPNP_PORT_MAPPING = 3600000;

    private final static int TIMEOUT_OUTBOUND_NODES = 10000;
    private final static int TIMEOUT_INBOUND_NODES = 10000;
    private final static int TIMEOUT_MSG_READ = 10000;

    private final static int MAX_CHANNEL_OUT_QUEUE = 8;

    private final int maxTempNodes;
    private final int maxConnections;
    private final int errTolerance;

    private final boolean syncSeedsOnly;
    private final boolean showStatus;
    private final boolean showLog;
    private final int selfNetId;
    private final String selfRevision;
    private final byte[] selfNodeId;
    private final int selfNodeIdHash;
    private final String selfShortId;
    private final byte[] selfIp;
    private final int selfPort;
    private final boolean upnpEnable;

    private final Map<Integer, List<Handler>> handlers = new ConcurrentHashMap<>();
    private final Set<Short> selfVersions = new HashSet<>();

    private final Set<String> seedIps = new HashSet<>();
    private final BlockingQueue<Node> tempNodes = new LinkedBlockingQueue<>();
    private final Map<Integer, Node> outboundNodes = new ConcurrentHashMap<>();
    private final Map<Integer, Node> inboundNodes = new ConcurrentHashMap<>();
    private final Map<Integer, Node> activeNodes = new ConcurrentHashMap<>();

    private AtomicBoolean start = new AtomicBoolean(true);
    private ServerSocketChannel tcpServer;
    private Selector selector;
    private final ThreadPoolExecutor kernelWorkers;
    private final ThreadPoolExecutor writeWorkers;
    private Thread tInbound, tShowStatus, tConnectPeers, tGetActivePeers, tClear;

    private final class TaskInbound implements Runnable {
        @Override
        public void run() {
            while (start.get()) {

                int num;
                try {
                    num = selector.select(1);
                } catch (IOException e) {
                    if (showLog)
                        System.out.println("<p2p inbound-select-io-exception>");
                    continue;
                }

                if (num == 0)
                    continue;

                Iterator<SelectionKey> keys = selector.selectedKeys().iterator();

                while (keys.hasNext()) {
                    SelectionKey sk = null;
                    try{
                        sk = keys.next();
                        keys.remove();
                        if (!sk.isValid()) continue;
                        if (sk.isAcceptable()) accept();
                        if (sk.isReadable()) read(sk);
                    } catch (IOException e){
                        closeSocket((SocketChannel) sk.channel());
                    } catch (Exception e){
                        e.printStackTrace();
                    }
                }
            }
            if (showLog)
                System.out.println("<p2p-pi shutdown>");
        }
    }

    private final class TaskDiscardPolicy implements RejectedExecutionHandler {
        TaskDiscardPolicy() {}
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            if(showLog)
                System.out.println("<p2p workers-queue-full>");
        }
    }

    private final class TaskStatus implements Runnable {
        @Override
        public void run() {
            Thread.currentThread().setName("p2p-ts");
            while(P2pMgr.this.start.get()){
                try{
                    StringBuilder sb = new StringBuilder();
                    sb.append("\n");
                    sb.append(String.format("================================================================== p2p-status-%6s ==================================================================\n", selfShortId));
                    sb.append(String.format("temp[%3d] inbound[%3d] outbound[%3d] active[%3d]                            s - seed node, td - total difficulty, # - block number, bv - binary version\n", tempNodes.size(), 0, 0, activeNodes.size()));
                    Collection<INode> sorted = getActiveNodes().values();
                    if (sorted.size() > 0) {
                        sb.append("\n          s");
                        sb.append("               td");
                        sb.append("          #");
                        sb.append("                                                             hash");
                        sb.append("              ip");
                        sb.append("  port");
                        sb.append("     conn");
                        sb.append("              bv\n");
                        sb.append(
                                "-------------------------------------------------------------------------------------------------------------------------------------------------------\n");
                        sorted.sort((n1, n2) -> {
                            int tdCompare = n2.getTotalDifficulty().compareTo(n1.getTotalDifficulty());
                            if (tdCompare == 0) {
                                Long n2Bn = n2.getBestBlockNumber();
                                Long n1Bn = n1.getBestBlockNumber();
                                return n2Bn.compareTo(n1Bn);
                            } else
                                return tdCompare;
                        });
                        for (INode n : sorted) {
                            try {
                                sb.append(
                                    String.format("id:%6s %c %16s %10d %64s %15s %5d %8s %15s\n",
                                        n.getIdShort(),
                                        n.getIfFromBootList() ? 'y' : ' ', n.getTotalDifficulty().toString(10),
                                        n.getBestBlockNumber(),
                                        n.getBestBlockHash() == null ? "" : Utility.bytesToHex(n.getBestBlockHash()), n.getIpStr(),
                                        n.getPort(),
                                        n.getConnection(),
                                        n.getBinaryVersion()
                                    )
                                );
                            } catch (Exception ex) {
                                ex.printStackTrace();
                            }
                        }
                    }
                    sb.append("\n");
                    System.out.println(sb.toString());
                    Thread.sleep(PERIOD_SHOW_STATUS);
                } catch (InterruptedException e){
                    if(showLog)
                        System.out.println("<p2p-ts shutdown>");
                    return;
                } catch (Exception e){
                    if(showLog)
                        e.printStackTrace();
                }
            }
        }
    }

    private final class TaskGetActiveNodes implements  Runnable {
        @Override
        public void run() {
            while(start.get()){
                INode node = getRandom();
                if (node != null)
                    send(node.getIdHash(), node.getIdShort(), new ReqActiveNodes());
                try {
                    Thread.sleep(PERIOD_REQUEST_ACTIVE_NODES);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }
    }

    private final class TaskConnect implements Runnable {

        //private final P2pMgr p2pMgr;

        // TaskConnect(final P2pMgr _p2pMgr){
        //    this.p2pMgr = _p2pMgr;
        //}

        @Override
        public void run() {
            Thread.currentThread().setName("p2p-tcp");
            while (start.get()) {
                try {
                    Thread.sleep(PERIOD_CONNECT_OUTBOUND);
                } catch (InterruptedException e) {
                    if (showLog)
                        System.out.println("<p2p-tcp interrupted>");
                }

                if (activeNodes.size() >= maxConnections) {
                    if (showLog)
                        System.out.println("<p2p-tcp-connect-peer pass max-active-nodes>");
                    return;
                }

                Node node;
                try {
                    node = tempNodes.take();
                    if (node.getIfFromBootList())
                        tempNodes.offer(node);
                } catch (InterruptedException e) {
                    if (showLog)
                        System.out.println("<p2p outbound-connect-io-exception>");
                    return;
                }
                int nodeIdHash = node.getIdHash();

                if (!outboundNodes.containsKey(nodeIdHash) && !activeNodes.containsKey(nodeIdHash)) {
                    int _port = node.getPort();
                    try {
                        SocketChannel channel = SocketChannel.open();
                        if (showLog)
                            System.out.println("<p2p try-connect-" + node.getIpStr() + ">");
                        channel.socket().connect(
                                new InetSocketAddress(node.getIpStr(), _port),
                                TIMEOUT_OUTBOUND_CONNECT
                        );
                        configChannel(channel);

                        if (channel.finishConnect() && channel.isConnected()) {
                            SelectionKey sk = channel.register(selector, SelectionKey.OP_READ);
                            ChannelBuffer rb = new ChannelBuffer(P2pMgr.this.showLog, P2pMgr.MAX_CHANNEL_OUT_QUEUE);
                            rb.setNodeIdHash(nodeIdHash);
                            sk.attach(rb);
                            node.setChannel(channel);
                            if(outboundNodes.putIfAbsent(nodeIdHash, node) == null){
                                workers.submit(
                                    new TaskWrite(
                                        workers,
                                        showLog,
                                        node.getIdShort(),
                                        channel,
                                        new ReqHandshake1(
                                            selfNodeId,
                                            selfNetId,
                                            selfIp,
                                            selfPort,
                                            selfRevision.getBytes(),
                                            new ArrayList<>(selfVersions)
                                        ),
                                        rb
                                    )
                                );
                                if (showLog)
                                    System.out.println("<p2p success-add-outbound addr=" + node.getIpStr() + ":" + _port);
                            } else {
                                channel.close();
                                if (showLog)
                                    System.out.println("<p2p fail-add-outbound error=exist addr=" + node.getIpStr() + ":" + _port);
                            }
                        } else {
                            channel.close();
                        }
                    } catch (IOException e) {
                        if (showLog)
                            System.out.println("<p2p action=connect-outbound addr=" + node.getIpStr() + ":" + _port
                                    + " result=failed>");
                    }
                }
            }
        }
    }

    private final class TaskClear implements Runnable {
        @Override
        public void run() {
            Thread.currentThread().setName("p2p-clr");
            while (start.get()) {
                try {
                    Thread.sleep(PERIOD_CLEAR);
                    timeout(inboundNodes, "inbound", TIMEOUT_INBOUND_NODES);
                    Thread.sleep(PERIOD_CLEAR);
                    timeout(inboundNodes, "outbound", TIMEOUT_OUTBOUND_NODES);
                    Thread.sleep(PERIOD_CLEAR);
                    timeoutActive();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * @param _nodeId         byte[36]
     * @param _ip             String
     * @param _port           int
     * @param _bootNodes      String[]
     * @param _upnpEnable     boolean
     * @param _maxTempNodes   int
     * @param _maxConnections int
     * @param _showStatus     boolean
     * @param _showLog        boolean
     */
    public P2pMgr(int _netId, String _revision, String _nodeId, String _ip, int _port, final String[] _bootNodes,
                  boolean _upnpEnable, int _maxTempNodes, int _maxConnections, boolean _showStatus, boolean _showLog,
                  boolean _bootlistSyncOnly, int _errorTolerance) {
        this.selfNetId = _netId;
        this.selfRevision = _revision;
        this.selfNodeId = _nodeId.getBytes();
        this.selfNodeIdHash = Arrays.hashCode(selfNodeId);
        this.selfShortId = new String(Arrays.copyOfRange(_nodeId.getBytes(), 0, 6));
        this.selfIp = Node.ipStrToBytes(_ip);
        this.selfPort = _port;
        this.upnpEnable = _upnpEnable;
        this.maxTempNodes = _maxTempNodes;
        this.maxConnections = Math.max(_maxConnections, 128);
        this.showStatus = _showStatus;
        this.showLog = _showLog;
        this.syncSeedsOnly = _bootlistSyncOnly;
        this.errTolerance = _errorTolerance;

        for (String _bootNode : _bootNodes) {
            Node node = Node.parseP2p(_bootNode);
            if (node != null && validateNode(node)) {
                tempNodes.add(node);
                seedIps.add(node.getIpStr());
            }
        }

        int cores = Runtime.getRuntime().availableProcessors();
        this.kernelWorkers = new ThreadPoolExecutor(
                cores,
                cores * 2,
                60,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(100),
                new ThreadFactory() {
                    private AtomicInteger count = new AtomicInteger();
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "kernel-worker-" + count.incrementAndGet());
                    }
                },
                new TaskDiscardPolicy()
        );
        this.writeWorkers = new ThreadPoolExecutor(
                cores,
                cores * 2,
                60,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(100),
                new ThreadFactory() {
                    private AtomicInteger count = new AtomicInteger();
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "write-worker-" + count.incrementAndGet());
                    }
                },
                new TaskDiscardPolicy()
        );
    }

    /**
     * @param _node Node
     * @return boolean
     */
    private boolean validateNode(final Node _node) {
        boolean notNull = _node != null;
        filter self
        boolean notSelfId = _node.getIdHash() != this.selfNodeIdHash;
        boolean notSameIpOrPort = !(Arrays.equals(selfIp, _node.getIp()) && selfPort == _node.getPort());

        filter already active.
        boolean notActive = !nodeMgr.hasActiveNode(_node.getIdHash());

        boolean notActive = !nodeMgr.hasActiveNode(_node.getChannelId());

        filter out conntected.
        boolean notOutbound = !(_node.st.stat == NodeState.CONNECTTED);

        for (Node n : this.nodeMgr.allStmNodes) {
            if (n.getIdHash() == _node.getIdHash()) {
                return false;
            }
        }

        return notNull && notSelfId && notSameIpOrPort && notActive && notOutbound;
    }

    /**
     * @param _channel SocketChannel TODO: check option
     */
    private void configChannel(final SocketChannel _channel) throws IOException {
        _channel.configureBlocking(false);
        _channel.socket().setSoTimeout(TIMEOUT_MSG_READ);
    }

    /**
     * @param _sc SocketChannel
     */
    private void closeSocket(final SocketChannel _sc) {
        if (showLog)
            System.out.println("<p2p close-socket->");

        try {
            SelectionKey sk = _sc.keyFor(selector);
            _sc.close();
            if (sk != null)
                sk.cancel();
        } catch (IOException e) {
            if (showLog)
                System.out.println("<p2p close-socket-io-exception>");
        }
    }

    /**
     * accept new connection
     */
    private void accept() {
        SocketChannel channel;
        try {
            channel = tcpServer.accept();
            configChannel(channel);

            SelectionKey sk = channel.register(selector, SelectionKey.OP_READ);

            String ip = channel.socket().getInetAddress().getHostAddress();
            int port = channel.socket().getPort();

            if (syncSeedsOnly && nodeMgr.isSeedIp(ip)) {
                close the channel and return.
                channel.close();
                return;
            }

            Node node = new Node(false, ip);
            Node node = nodeMgr.allocNode(ip, 0, port);

            node.setChannel(channel);
            node.st.setStat(NodeState.ACCEPTED);
            node.st.setStatOr(NodeState.HS);

            ChannelBuffer cb = new ChannelBuffer();
            cb.set
            sk.attach(cb);

            if (showLog)
                System.out.println("<p2p new-connection " + ip + ":" + port + ">");

        } catch (IOException e) {
            if (showLog)
                System.out.println("<p2p inbound-accept-io-exception>");
            return;
        }
    }

    /**
     * @param _sc SocketChannel
     * @throws IOException IOException
     */
    private void readHeader(final SocketChannel _sc, final ChannelBuffer _cb) throws IOException {

        int ret;
        while ((ret = _sc.read(_cb.headerBuf)) > 0) {
        }

        if (!_cb.headerBuf.hasRemaining()) {
            _cb.header = Header.decode(_cb.headerBuf.array());
        } else {
            if (ret == -1) {
                throw new IOException("read-header-eof");
            }
        }
    }

    /**
     * @param _sc SocketChannel
     * @throws IOException IOException
     */
    private void readBody(final SocketChannel _sc, final ChannelBuffer _cb) throws IOException {

        if (_cb.bodyBuf == null)
            _cb.bodyBuf = ByteBuffer.allocate(_cb.header.getLen());

        int ret;
        while ((ret = _sc.read(_cb.bodyBuf)) > 0) {
        }

        if (!_cb.bodyBuf.hasRemaining()) {
            _cb.body = _cb.bodyBuf.array();
        } else {
            if (ret == -1) {
                throw new IOException("read-body-eof");
            }
        }
    }

    /**
     * @param _sk SelectionKey
     * @throws IOException IOException
     */
    private void read(final SelectionKey _sk) throws IOException {

        if (_sk.attachment() == null) {
            throw new IOException("attachment is null");
        }
        ChannelBuffer rb = (ChannelBuffer) _sk.attachment();

        read header
        if (!rb.isHeaderCompleted()) {
            readHeader((SocketChannel) _sk.channel(), rb);
        }

        read body
        if (rb.isHeaderCompleted() && !rb.isBodyCompleted()) {
            readBody((SocketChannel) _sk.channel(), rb);
        }

        if (!rb.isBodyCompleted())
            return;

        Header h = rb.header;
        byte[] bodyBytes = rb.body;
        rb.refreshHeader();
        rb.refreshBody();

        short ver = h.getVer();
        byte ctrl = h.getCtrl();
        byte act = h.getAction();

        print route
        System.out.println("read " + ver + "-" + ctrl + "-" + act);
        switch (ver) {
            case Ver.V0:
                switch (ctrl) {
                    case Ctrl.NET:
                        handleP2pMsg(_sk, act, bodyBytes);
                        break;
                    default:
                        int route = h.getRoute();
                        if (handlers.containsKey(route))
                            handleKernelMsg(_sk.channel().hashCode(), route, bodyBytes);
                        break;
                }
                break;
        }
    }

    /**
     * @return boolean
     * TODO: supported protocol selfVersions
     */
    private boolean handshakeRuleCheck(int netId) {
        return netId == selfNetId;
    }

    /**
     * @param _buffer      ChannelBuffer
     * @param _channelHash int
     * @param _nodeId      byte[]
     * @param _netId       int
     * @param _port        int
     * @param _revision    byte[]
     *                     Construct node info after handshake request success
     */
    private void handleReqHandshake(
            final ChannelBuffer _buffer,
            int _channelId,
            final byte[] _nodeId,
            int _netId,
            int _port,
            final byte[] _revision
    ) {
        Node node = connections.get(_channelId);
        if (node != null &&) {
            if (handshakeRuleCheck(_netId)) {

                node.setId(_nodeId);
                node.setPort(_port);

                String binaryVersion;
                try {
                    binaryVersion = new String(_revision, "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    binaryVersion = "decode-fail";
                }
                node.setBinaryVersion(binaryVersion);
                workers.submit(new TaskWrite(workers, showLog,
                        node.getIdShort(), node.getChannel(),
                        cachedResHandshake, _buffer, this));
                sendMsgQue.offer(new MsgOut(node.getChannelId(), cachedResHandshake, Dest.INBOUND));

                node.st.setStat(NodeState.HS_DONE);

                node.st.setStat(NodeState.ACTIVE);

            } else {
                if (showLog)
                    System.out.println("incompatible netId ours=" + this.selfNetId + " theirs=" + _netId);
            }
        }
    }

    private void handleResHandshake(int channelIdHash, String _binaryVersion) {

        Node node = nodeMgr.getStmNode(channelIdHash, NodeState.CONNECTTED);

        if (node != null) {

            node.refreshTimestamp();
            node.setBinaryVersion(_binaryVersion);

            node.st.setStat(NodeState.HS_DONE);

            node.st.setStat(NodeState.ACTIVE);

        }
    }

    /**
     * @param _sk       SelectionKey
     * @param _act      ACT
     * @param _msgBytes byte[]
     */
    private void handleP2pMsg(final SelectionKey _sk, byte _act, final byte[] _msgBytes) {

        ChannelBuffer rb = (ChannelBuffer) _sk.attachment();

        switch (_act) {

            case Act.REQ_HANDSHAKE:
                ReqHandshake1 reqHandshake1 = ReqHandshake1.decode(_msgBytes);

                if (reqHandshake1 != null) {
                    handleReqHandshake(rb, _sk.channel().hashCode(), reqHandshake1.getNodeId(),
                            reqHandshake1.getNetId(), reqHandshake1.getPort(), reqHandshake1.getRevision());
                }
                break;

            case Act.RES_HANDSHAKE:
                ResHandshake1 resHandshake1 = ResHandshake1.decode(_msgBytes);
                if (resHandshake1 != null && resHandshake1.getSuccess())
                    handleResHandshake(rb.cid, resHandshake1.getBinaryVersion());
                break;

            case Act.REQ_ACTIVE_NODES:
                if (rb.cid <= 0)
                    return;

                Node node = nodeMgr.getActiveNode(rb.cid);
                if (node != null)
                    workers.submit(new TaskWrite(workers, showLog,
                            node.getIdShort(), node.getChannel(),
                            new ResActiveNodes(new ArrayList<Node>(activeNodes.values())), rb,
                            this));
                break;

            case Act.RES_ACTIVE_NODES:
                if (syncSeedsOnly)
                    break;

                if (rb.cid > 0) {
                    Node node = nodeMgr.getActiveNode(rb.cid);
                    if (node != null) {
                        node.refreshTimestamp();
                        ResActiveNodes resActiveNodes = ResActiveNodes.decode(_msgBytes);
                        if (resActiveNodes != null) {
                            List<Node> incomingNodes = resActiveNodes.getNodes();
                            for (Node incomingNode : incomingNodes) {
                                if (nodeMgr.tempNodesSize() >= this.maxTempNodes)
                                    return;
                                if (validateNode(incomingNode))
                                    nodeMgr.tempNodesAdd(incomingNode);
                            }
                        }
                    }
                }
                break;
            default:
                if (showLog)
                    System.out.println("<p2p unknown-route act=" + _act + ">");
                break;
        }
    }

    /**
     * @param _channelId int
     * @param _route     int
     * @param _msgBytes  byte[]
     */
    private void handleKernelMsg(int _id, int _route, final byte[] _msgBytes) {
        Optional<Map.Entry<Integer, Node>> entry = connections.entrySet().stream().filter(e -> e.getValue().state.hasStat(NodeState.ACTIVE)).findAny();
        if (entry != null && entry.isPresent()) {
            Node node = entry.get().getValue();
            List<Handler> hs = handlers.get(_route);
            if (hs == null)
                return;
            for (Handler hlr : hs) {
                if (hlr == null)
                    continue;
                node.refreshTimestamp();
                System.out.println("I am handle kernel msg !!!!! " + hlr.getHeader().getCtrl() + "-" + hlr.getHeader().getAction() + "-" + hlr.getHeader().getLen());
                workers.submit(() -> hlr.receive(node.getIdHash(), node.getIdShort(), _msgBytes));
            }
        } else {
            if (showLog)
                System.out.println("<p2p-handle-kernel-msg-failed channel-id=" + _channelId + ">");

        }
    }

    private void timeout(final Map<Integer, Node> _collection, String _collectionName, int _timeout) {
        Iterator<Map.Entry<Integer, Node>> inboundIt = _collection.entrySet().iterator();
        while (inboundIt.hasNext()) {
            try{
                Node node = inboundIt.next().getValue();
                if (System.currentTimeMillis() - node.getTimestamp() > _timeout) {
                    closeSocket(node.getChannel(), _collectionName + "-timeout ip=" + node.getIpStr());
                    inboundIt.remove();
                }
            } catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    private void timeoutActive() {
        long now = System.currentTimeMillis();

        OptionalDouble average = activeNodes.values().stream().mapToLong(n -> now - n.getTimestamp()).average();
        double timeout = average.orElse(4000) * 5;
        timeout = Math.max(10000, Math.min(timeout, 60000));
        if (showLog)
            System.out.printf("<p2p average-delay=%.0fms>\n", average.orElse(0));

        Iterator<Map.Entry<Integer, Node>> activeIt = activeNodes.entrySet().iterator();
        while (activeIt.hasNext()) {
            Node node = activeIt.next().getValue();

            if (now - node.getTimestamp() > timeout) {
                closeSocket(node.getChannel(), "active-timeout node=" + node.getIdShort() + " ip=" + node.getIpStr());
                activeIt.remove();
            }

            if (!node.getChannel().isConnected()) {
                closeSocket(node.getChannel(), "channel-already-closed node=" + node.getIdShort() + " ip=" + node.getIpStr());
                activeIt.remove();
            }
        }
    }

    @Override
    public void run() {
        try {
            selector = Selector.open();

            tcpServer = ServerSocketChannel.open();
            tcpServer.configureBlocking(false);
            tcpServer.socket().setReuseAddress(true);
            tcpServer.socket().bind(new InetSocketAddress(Node.ipBytesToStr(selfIp), selfPort));
            tcpServer.register(selector, SelectionKey.OP_ACCEPT);

            tInbound = new Thread(new TaskInbound(), "p2p-inbound");
            tInbound.setPriority(Thread.NORM_PRIORITY);
            tInbound.start();

            tConnectPeers = new Thread(new TaskConnect(), "p2p-connect");
            tConnectPeers.setPriority(Thread.NORM_PRIORITY);
            tConnectPeers.start();

            tClear = new Thread(new TaskClear(), "p2p-clear");
            tClear.setPriority(Thread.NORM_PRIORITY);
            tClear.start();

            if (showStatus) {
                tShowStatus = new Thread(new TaskStatus(), "p2p-status");
                tShowStatus.setPriority(Thread.NORM_PRIORITY);
                tShowStatus.run();
            }

            if (!syncSeedsOnly){
                tGetActivePeers = new Thread(new TaskGetActiveNodes(), "p2p-active-peers");
                tGetActivePeers.setPriority(Thread.NORM_PRIORITY);
                tGetActivePeers.run();
            }

        } catch (IOException e) {
            if (showLog)
                System.out.println("<p2p tcp-server-io-exception>");
        }
    }

    @Override
    public INode getRandom() {
        int nodesCount = activeNodes.size();
        if (nodesCount > 0) {
            Random r = new Random(System.currentTimeMillis());
            List<Integer> keysArr = new ArrayList<>(activeNodes.keySet());
            try {
                int randomNodeKeyIndex = r.nextInt(keysArr.size());
                int randomNodeKey = keysArr.get(randomNodeKeyIndex);
                return activeNodes.get(randomNodeKey);
            } catch (Exception e) {
                System.out.println("<p2p get-random-exception>");
                return null;
            }
        } else
            return null;
    }

    @Override
    public Map<Integer, INode> getActiveNodes() {
        return new HashMap<>(activeNodes);
    }

    @Override
    public void register(final List<Handler> _cbs) {
        for (Handler _cb : _cbs) {
            Header h = _cb.getHeader();
            short ver = h.getVer();
            byte ctrl = h.getCtrl();
            if (Ver.filter(ver) != Ver.UNKNOWN && Ctrl.filter(ctrl) != Ctrl.UNKNOWN) {
                if (!selfVersions.contains(ver)) {
                    selfVersions.add(ver);
                }

                int route = h.getRoute();
                List<Handler> routeHandlers = handlers.get(route);
                if (routeHandlers == null) {
                    routeHandlers = new ArrayList<>();
                    routeHandlers.add(_cb);
                    handlers.put(route, routeHandlers);
                } else {
                    routeHandlers.add(_cb);
                }
            }
        }
    }

    @Override
    public void send(int _id, String _displayId, final Msg _msg) {
        Node node = activeNodes.get(_id);
        if (node != null) {
            SelectionKey sk = node.getChannel().keyFor(selector);
            if (sk != null) {
                Object attachment = sk.attachment();
                if (attachment != null)
                    writeWorkers.submit(
                            new TaskWrite(writeWorkers, showLog, node.getIdShort(), node.getChannel(), _msg, (ChannelBuffer) attachment));
            }
        }
    }

    @Override
    public void shutdown() {
        start.set(false);
        if(tInbound != null) tInbound.interrupt();
        kernelWorkers.shutdown();
        if(tConnectPeers != null) tConnectPeers.interrupt();
        if(tGetActivePeers != null) tGetActivePeers.interrupt();
        if(tShowStatus != null) tShowStatus.interrupt();
        if(tClear != null) tClear.interrupt();
        writeWorkers.shutdown();
        for (List<Handler> hdrs : handlers.values()) {
            hdrs.forEach(Handler::shutDown);
        }
    }

    @Override
    public List<Short> versions() {
        return new ArrayList<>(selfVersions);
    }

    @Override
    public int chainId() {
        return selfNetId;
    }

    @Override
    public int getSelfIdHash() {
        return this.selfNodeIdHash;
    }

    @Override
    public void closeSocket(SocketChannel _sc, String _reason) {

    }

    @Override
    public boolean isShowLog() {
        return this.showLog;
    }

    @Override
    public void errCheck(int nodeIdHashcode, String _displayId) {
        // TODO
    }
}

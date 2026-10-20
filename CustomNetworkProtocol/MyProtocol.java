import client.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Custom L2/L3 protocol: token-ring MAC, heartbeat-based topology mapping with
 * ID-collision resolution, 29-byte payload fragmentation with sentinel padding,
 * and stop-and-wait ARQ for reliable delivery.
 */
public class MyProtocol {

    // Hardware / server parameters.
    private static String SERVER_IP = "netsys.ewi.utwente.nl";
    private static int SERVER_PORT = 8954;
    private static int frequency = 2600;
    private String token = "java-21-E5495E4CE445DD33A2";

    // Protocol constants.
    private static final int MAX_ID               = 7;   // IDs 1..7 (3-bit field, 0 reserved)
    private static final int FRAME_SIZE           = 32;
    private static final int PAYLOAD_SIZE         = 29;  // 32 - 2 header - 1 sentinel
    private static final byte PAD_SENTINEL        = (byte) 128;
    private static final long HB_MIN_MS           = 2000;
    private static final long HB_MAX_MS           = 2500;
    private static final long HB_STALE_MS         = 30_000;
    private static final long HB_MONITOR_MS       = 5_000;
    private static final long ARQ_RETRY_MS        = 2_000;
    private static final long TOKEN_RETRY_MS      = 1_000;
    private static final int  TOKEN_MAX_RETRIES   = 4;
    private static final long TOKEN_RESURRECT_MS  = 10_000;

    // Queues.
    private final BlockingQueue<Message> receivedQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<Message> sendingQueue  = new LinkedBlockingQueue<>();
    private final BlockingQueue<Message> localQueue    = new LinkedBlockingQueue<>();

    // Node identity. Guarded by {@link #idLock} for multi-field atomicity.
    private final Object idLock = new Object();
    private final AtomicInteger sourceId = new AtomicInteger();
    private final AtomicInteger nonce    = new AtomicInteger();
    private final AtomicInteger bitmap   = new AtomicInteger();

    // Topology.
    private final ConcurrentHashMap<Integer, Integer> routingTable = new ConcurrentHashMap<>(); // destId -> nextHop
    private final ConcurrentHashMap<Integer, Long>    heartbeatHistory = new ConcurrentHashMap<>();
    private final AtomicLong lastTopologyChange = new AtomicLong(System.currentTimeMillis());
    private final AtomicLong lastTokenSeen      = new AtomicLong(System.currentTimeMillis());

    // Token state.
    private final AtomicBoolean hasToken       = new AtomicBoolean(false);
    private final AtomicBoolean tokenAcked     = new AtomicBoolean(false);
    private final AtomicInteger tokenRetries   = new AtomicInteger(0);
    private final AtomicInteger currentTokenDest = new AtomicInteger(-1);

    // Sender / ARQ state.
    private final AtomicInteger seqCounter = new AtomicInteger(0);
    private final ConcurrentHashMap<String, Message> arqCache = new ConcurrentHashMap<>();
    private final AtomicBoolean awaitingAck = new AtomicBoolean(false);
    private volatile String currentArqKey = null;

    // Reassembly: sourceId -> (seq -> fragment bytes).
    private final ConcurrentHashMap<Integer, NavigableMap<Integer, byte[]>> reassembly = new ConcurrentHashMap<>();

    public MyProtocol(String ip, int port, int freq) {
        synchronized (idLock) {
            int id = 1 + new Random().nextInt(MAX_ID);
            sourceId.set(id);
            nonce.set(new Random().nextInt(14));
            bitmap.set(1 << (id - 1));
        }

        new Client(ip, port, freq, token, receivedQueue, sendingQueue);

        new ReceiveThread().start();
        new HeartbeatThread().start();
        new HeartbeatMonitorThread().start();
        new TokenManagerThread().start();
        new ArqRetransmitThread().start();

        runStdinLoop();
    }

    public static void main(String[] args) {
        if (args.length > 0) frequency = Integer.parseInt(args[0]);
        new MyProtocol(SERVER_IP, SERVER_PORT, frequency);
    }

    // ------------------------------------------------------------------ stdin

    private void runStdinLoop() {
        try {
            ByteBuffer temp = ByteBuffer.allocate(1024);
            while (true) {
                int read = System.in.read(temp.array());
                if (read <= 0) continue;

                int trim = 0;
                if (read >= 1 && (temp.get(read - 1) == '\n' || temp.get(read - 1) == '\r')) trim++;
                if (read >= 2 && (temp.get(read - 2) == '\n' || temp.get(read - 2) == '\r')) trim++;
                int dataSize = read - trim;
                if (dataSize <= 0) continue;

                int dest = pickDestination();
                if (dest <= 0) {
                    System.out.println("[send] no known destination yet; dropping input");
                    continue;
                }
                int nextHop = routingTable.getOrDefault(dest, dest);

                int fragCount = (dataSize + PAYLOAD_SIZE - 1) / PAYLOAD_SIZE;
                for (int i = 0; i < fragCount; i++) {
                    int offset    = i * PAYLOAD_SIZE;
                    int fragLen   = Math.min(PAYLOAD_SIZE, dataSize - offset);
                    boolean last  = (i == fragCount - 1);
                    int seq       = seqCounter.getAndIncrement() & 0x0F;

                    PayloadHeader h = new PayloadHeader(false, sourceId.get(), dest, last, nextHop, seq);
                    ByteBuffer frame = ByteBuffer.allocate(FRAME_SIZE);
                    frame.putShort((short) h.assemble());
                    frame.put(temp.array(), offset, fragLen);

                    int padStart = Header.HEADER_SIZE + fragLen;
                    if (padStart < FRAME_SIZE) {
                        frame.put(padStart, PAD_SENTINEL);
                        // remaining bytes already zero from allocate()
                    }
                    localQueue.put(new Message(MessageType.DATA, frame));
                }
            }
        } catch (InterruptedException | IOException e) {
            System.exit(2);
        }
    }

    private int pickDestination() {
        int me = sourceId.get();
        int best = -1;
        for (Integer dst : routingTable.keySet()) {
            if (dst == me) continue;
            if (best == -1 || dst < best) best = dst;
        }
        return best;
    }

    // --------------------------------------------------------------- sending

    private void enqueueShort(Header h) {
        ByteBuffer buf = ByteBuffer.allocate(2);
        buf.putShort((short) h.assemble());
        try { sendingQueue.put(new Message(MessageType.DATA_SHORT, buf)); }
        catch (InterruptedException ignored) {}
    }

    private void enqueueFrame(Message m) {
        try { sendingQueue.put(m); } catch (InterruptedException ignored) {}
    }

    private static String arqKey(int srcId, int destId, int seq) {
        return seq + ";" + srcId + ":" + destId;
    }

    // ------------------------------------------------------------ heartbeats

    private class HeartbeatThread extends Thread {
        HeartbeatThread() { super("hb-send"); setDaemon(true); }
        public void run() {
            Random r = new Random();
            while (true) {
                sendHeartbeat(false);
                try { Thread.sleep(HB_MIN_MS + (long) r.nextInt((int)(HB_MAX_MS - HB_MIN_MS + 1))); }
                catch (InterruptedException e) { return; }
            }
        }
    }

    private void sendHeartbeat(boolean collision) {
        HeartbeatHeader h;
        synchronized (idLock) {
            h = new HeartbeatHeader(sourceId.get(), nonce.get(), collision, bitmap.get() & 0x7F);
        }
        enqueueShort(h);
    }

    private class HeartbeatMonitorThread extends Thread {
        HeartbeatMonitorThread() { super("hb-monitor"); setDaemon(true); }
        public void run() {
            while (true) {
                try { Thread.sleep(HB_MONITOR_MS); }
                catch (InterruptedException e) { return; }

                long now = System.currentTimeMillis();
                Set<Integer> toDrop = new HashSet<>();
                for (Map.Entry<Integer, Long> e : heartbeatHistory.entrySet()) {
                    if (now - e.getValue() > HB_STALE_MS) toDrop.add(e.getKey());
                }
                if (toDrop.isEmpty()) continue;

                boolean changed = false;
                synchronized (idLock) {
                    int bm = bitmap.get();
                    for (Integer gone : toDrop) {
                        heartbeatHistory.remove(gone);
                        if ((bm & (1 << (gone - 1))) != 0) {
                            bm &= ~(1 << (gone - 1));
                            changed = true;
                        }
                        // Any routing entry whose nextHop is this gone node must go too.
                        Iterator<Map.Entry<Integer, Integer>> it = routingTable.entrySet().iterator();
                        while (it.hasNext()) {
                            Map.Entry<Integer, Integer> rt = it.next();
                            if (rt.getKey().equals(gone) || rt.getValue().equals(gone)) {
                                int dst = rt.getKey();
                                if ((bm & (1 << (dst - 1))) != 0) {
                                    bm &= ~(1 << (dst - 1));
                                    changed = true;
                                }
                                it.remove();
                            }
                        }
                    }
                    bm |= (1 << (sourceId.get() - 1));
                    bitmap.set(bm);
                }
                if (changed) {
                    lastTopologyChange.set(System.currentTimeMillis());
                    sendHeartbeat(false);
                }
            }
        }
    }

    private boolean isNewcomer(int bm) {
        return Integer.bitCount(bm & 0x7F) == 1;
    }

    private void pickNewRandomId(int occupiedBitmap) {
        int occupied = occupiedBitmap & 0x7F;
        if (occupied == 0x7F) {
            System.err.println("All IDs occupied; exiting.");
            System.exit(2);
        }
        Random r = new Random();
        int id;
        do { id = 1 + r.nextInt(MAX_ID); } while (((occupied >>> (id - 1)) & 1) == 1);
        synchronized (idLock) {
            sourceId.set(id);
            nonce.set(r.nextInt(14));
            bitmap.set(occupied | (1 << (id - 1)));
        }
    }

    /**
     * Apply the incoming bitmap to our local view. For any bit set in
     * {@code incomingBitmap} that isn't in our bitmap, add that ID to the
     * routing table with {@code incomingSource} as nextHop (the neighbour that
     * just told us about it). Returns true if our bitmap changed.
     */
    private boolean mergeBitmap(int incomingBitmap, int incomingSource) {
        boolean changed = false;
        synchronized (idLock) {
            int me = sourceId.get();
            int myBm = bitmap.get();
            int newBm = myBm;
            for (int i = 0; i < MAX_ID; i++) {
                int mask = 1 << i;
                int id = i + 1;
                if (id == me) continue;
                boolean mine = (myBm & mask) != 0;
                boolean theirs = (incomingBitmap & mask) != 0;
                if (theirs && !mine) {
                    newBm |= mask;
                    routingTable.put(id, incomingSource);
                    changed = true;
                }
            }
            if (changed) bitmap.set(newBm);
        }
        return changed;
    }

    // --------------------------------------------------------------- token

    private int findNextTokenBearer() {
        int me = sourceId.get();
        List<Integer> active = new ArrayList<>(routingTable.keySet());
        active.add(me);
        Collections.sort(active);
        int idx = active.indexOf(me);
        if (idx < 0 || active.size() == 1) return -1;
        return active.get((idx + 1) % active.size());
    }

    private void transmitToken(int dest) {
        int nextHop = routingTable.getOrDefault(dest, dest);
        currentTokenDest.set(dest);
        TokenHeader h = new TokenHeader(sourceId.get(), TokenHeader.NONCE_PASS, nextHop, dest);
        enqueueShort(h);
    }

    private void transmitTokenAck(int originalSender) {
        int nextHop = routingTable.getOrDefault(originalSender, originalSender);
        TokenHeader ack = new TokenHeader(sourceId.get(), TokenHeader.NONCE_ACK, nextHop, originalSender);
        enqueueShort(ack);
    }

    private class TokenManagerThread extends Thread {
        TokenManagerThread() { super("token-mgr"); setDaemon(true); }
        public void run() {
            while (true) {
                try { Thread.sleep(200); } catch (InterruptedException e) { return; }

                // Resurrection: if topology stable AND no token seen for 10s AND
                // we are the lowest known ID -> regenerate.
                long now = System.currentTimeMillis();
                if (!hasToken.get()
                        && now - lastTopologyChange.get() > TOKEN_RESURRECT_MS
                        && now - lastTokenSeen.get()      > TOKEN_RESURRECT_MS) {
                    int me = sourceId.get();
                    int lowest = me;
                    for (Integer id : routingTable.keySet()) if (id < lowest) lowest = id;
                    if (lowest == me) {
                        System.out.println("[token] resurrecting token at node " + me);
                        hasToken.set(true);
                        lastTokenSeen.set(now);
                    }
                }

                // If we hold the token:
                //   - while we have pending data and no ACK outstanding, send next fragment
                //   - when local queue drained & no ACK outstanding, pass the token
                if (hasToken.get() && !awaitingAck.get()) {
                    Message next = localQueue.poll();
                    if (next != null) {
                        sendWithArq(next);
                    } else {
                        // Nothing to send: pass token.
                        int dest = findNextTokenBearer();
                        if (dest == -1) {
                            // Alone in the network: keep token silently.
                            continue;
                        }
                        tokenAcked.set(false);
                        tokenRetries.set(0);
                        transmitToken(dest);
                        scheduleTokenRetry(dest);
                    }
                }
            }
        }
    }

    private void sendWithArq(Message m) {
        Header h = Header.parse(Utils.byteBufferClone(m.getData()));
        if (!(h instanceof PayloadHeader)) { enqueueFrame(m); return; }
        PayloadHeader ph = (PayloadHeader) h;
        String key = arqKey(ph.getSourceId(), ph.getDestId(), ph.getSequenceNumber());
        arqCache.put(key, m);
        currentArqKey = key;
        awaitingAck.set(true);
        enqueueFrame(cloneMessage(m));
    }

    private static Message cloneMessage(Message m) {
        return new Message(m.getType(), Utils.byteBufferClone(m.getData()));
    }

    private void scheduleTokenRetry(int dest) {
        Utils.Timeout.setTimeout("token-retry", dest, TOKEN_RETRY_MS, data -> {
            if (tokenAcked.get()) return;
            int d = (Integer) data;
            int retries = tokenRetries.incrementAndGet();
            if (retries > TOKEN_MAX_RETRIES) {
                // Give up on this successor; try the next one.
                routingTable.remove(d);
                int nxt = findNextTokenBearer();
                if (nxt == -1) { hasToken.set(true); return; }
                tokenRetries.set(0);
                transmitToken(nxt);
                scheduleTokenRetry(nxt);
                return;
            }
            transmitToken(d);
            scheduleTokenRetry(d);
        });
    }

    // -------------------------------------------------------------- ARQ

    private class ArqRetransmitThread extends Thread {
        ArqRetransmitThread() { super("arq-retx"); setDaemon(true); }
        public void run() {
            while (true) {
                try { Thread.sleep(ARQ_RETRY_MS); }
                catch (InterruptedException e) { return; }

                if (!awaitingAck.get()) continue;
                String key = currentArqKey;
                if (key == null) continue;
                Message pending = arqCache.get(key);
                if (pending == null) continue;
                enqueueFrame(cloneMessage(pending));
            }
        }
    }

    // ------------------------------------------------------------ receive

    private class ReceiveThread extends Thread {
        ReceiveThread() { super("recv"); setDaemon(true); }

        public void run() {
            while (true) {
                try {
                    Message m = receivedQueue.take();
                    switch (m.getType()) {
                        case BUSY:  /* medium busy */ break;
                        case FREE:  /* medium free */ break;
                        case DATA:          handleFrame(m); break;
                        case DATA_SHORT:    handleFrame(m); break;
                        case DONE_SENDING:  break;
                        case HELLO:         break;
                        case SENDING:       break;
                        case END:           System.exit(0); break;
                        case TOKEN_ACCEPTED: break;
                        case TOKEN_REJECTED: break;
                        default: break;
                    }
                } catch (InterruptedException e) {
                    return;
                }
            }
        }

        private void handleFrame(Message m) {
            ByteBuffer dup = Utils.byteBufferClone(m.getData());
            Header h = Header.parse(dup);
            if (h instanceof HeartbeatHeader) {
                handleHeartbeat((HeartbeatHeader) h);
            } else if (h instanceof TokenHeader) {
                handleToken((TokenHeader) h);
            } else if (h instanceof PayloadHeader) {
                handlePayload((PayloadHeader) h, m);
            }
        }

        private void handleHeartbeat(HeartbeatHeader hh) {
            int incomingId   = hh.getSourceId();
            int incomingNon  = hh.getNonce();
            int incomingBm   = hh.getBitmap() & 0x7F;
            boolean collide  = hh.isCollision();

            heartbeatHistory.put(incomingId, System.currentTimeMillis());

            int me;
            int myNonce;
            int myBm;
            synchronized (idLock) {
                me = sourceId.get();
                myNonce = nonce.get();
                myBm = bitmap.get();
            }
            boolean amNew = isNewcomer(myBm);

            // ID collision resolution.
            if (incomingId == me) {
                if (amNew) {
                    // Newcomer yields unconditionally.
                    pickNewRandomId(incomingBm);
                    mergeBitmap(incomingBm, incomingId);
                    routingTable.put(incomingId, incomingId);
                    lastTopologyChange.set(System.currentTimeMillis());
                    sendHeartbeat(false);
                } else if (isNewcomer(incomingBm)) {
                    // We are established, they are a newcomer: reject them.
                    sendHeartbeat(true);
                } else if (myNonce > incomingNon) {
                    // We keep the ID; force them to yield.
                    sendHeartbeat(true);
                } else if (myNonce == incomingNon) {
                    // Both yield.
                    pickNewRandomId(myBm);
                    lastTopologyChange.set(System.currentTimeMillis());
                    sendHeartbeat(false);
                } else {
                    // They keep the ID; we yield.
                    pickNewRandomId(incomingBm);
                    mergeBitmap(incomingBm, incomingId);
                    routingTable.put(incomingId, incomingId);
                    lastTopologyChange.set(System.currentTimeMillis());
                    sendHeartbeat(false);
                }
                return;
            }

            // We are being told to yield by the established neighbour.
            if (collide && amNew) {
                pickNewRandomId(incomingBm);
                mergeBitmap(incomingBm, incomingId);
                routingTable.put(incomingId, incomingId);
                lastTopologyChange.set(System.currentTimeMillis());
                sendHeartbeat(false);
                return;
            }

            // Normal learning: merge their bitmap into ours.
            routingTable.put(incomingId, incomingId);
            boolean changed = mergeBitmap(incomingBm, incomingId);
            if (changed) {
                lastTopologyChange.set(System.currentTimeMillis());
                sendHeartbeat(false);
            }
        }

        private void handleToken(TokenHeader th) {
            lastTokenSeen.set(System.currentTimeMillis());
            int me = sourceId.get();
            if (th.getNextHop() != me) return; // not for us

            if (th.getNonce() == TokenHeader.NONCE_PASS) {
                if (th.getDestId() == me) {
                    // We are the target: ACK and take the token.
                    transmitTokenAck(th.getSourceId());
                    hasToken.set(true);
                } else {
                    // We are a relay. Forward with updated nextHop.
                    int nh = routingTable.getOrDefault(th.getDestId(), th.getDestId());
                    th.setNextHop(nh);
                    th.setSourceId(me);
                    enqueueShort(th);
                }
            } else if (th.getNonce() == TokenHeader.NONCE_ACK) {
                if (th.getDestId() == me) {
                    Utils.Timeout.cancel("token-retry");
                    tokenAcked.set(true);
                    hasToken.set(false);
                } else {
                    int nh = routingTable.getOrDefault(th.getDestId(), th.getDestId());
                    th.setNextHop(nh);
                    th.setSourceId(me);
                    enqueueShort(th);
                }
            }
        }

        private void handlePayload(PayloadHeader ph, Message m) {
            int me = sourceId.get();

            if (ph.isAck()) {
                if (ph.getDestId() == me) {
                    // ACK for a fragment we sent.
                    String key = arqKey(ph.getDestId(), ph.getSourceId(), ph.getSequenceNumber());
                    // ^ The ACK swapped src/dst, so our cached key used (our-src, their-dst).
                    // But the cached key was our original (me, destInDataPacket). Recompute:
                    String myCachedKey = arqKey(me, ph.getSourceId(), ph.getSequenceNumber());
                    arqCache.remove(myCachedKey);
                    if (myCachedKey.equals(currentArqKey)) {
                        currentArqKey = null;
                        awaitingAck.set(false);
                    }
                } else if (ph.getNextHop() == me) {
                    // Relay the ACK onwards.
                    int nh = routingTable.getOrDefault(ph.getDestId(), ph.getDestId());
                    ph.setNextHop(nh);
                    ByteBuffer fresh = ByteBuffer.allocate(FRAME_SIZE);
                    fresh.putShort((short) ph.assemble());
                    fresh.position(2);
                    ByteBuffer src = Utils.byteBufferClone(m.getData());
                    src.position(2);
                    while (fresh.position() < FRAME_SIZE && src.hasRemaining()) fresh.put(src.get());
                    enqueueFrame(new Message(m.getType(), fresh));
                }
                return;
            }

            if (ph.getDestId() == me) {
                // We are the final destination: store fragment and ACK.
                byte[] bytes = extractFragmentBytes(m.getData());
                reassembly
                    .computeIfAbsent(ph.getSourceId(), k -> new ConcurrentSkipListMap<>())
                    .put(ph.getSequenceNumber(), bytes);

                sendPayloadAck(ph);

                if (ph.isLastFragment()) {
                    NavigableMap<Integer, byte[]> frags = reassembly.remove(ph.getSourceId());
                    if (frags != null) {
                        ByteBuffer total = ByteBuffer.allocate(frags.size() * PAYLOAD_SIZE);
                        for (byte[] b : frags.values()) total.put(b);
                        total.flip();
                        byte[] out = new byte[total.remaining()];
                        total.get(out);
                        System.out.println("[recv] from " + ph.getSourceId() + ": " + new String(out));
                    }
                }
            } else if (ph.getNextHop() == me) {
                // Relay payload: rewrite nextHop, keep everything else identical.
                int nh = routingTable.getOrDefault(ph.getDestId(), ph.getDestId());
                ph.setNextHop(nh);
                ByteBuffer src = Utils.byteBufferClone(m.getData());
                ByteBuffer fresh = ByteBuffer.allocate(FRAME_SIZE);
                fresh.putShort((short) ph.assemble());
                src.position(2);
                while (fresh.position() < FRAME_SIZE && src.hasRemaining()) fresh.put(src.get());
                enqueueFrame(new Message(m.getType(), fresh));
            }
        }

        /**
         * Strip header (2 bytes) and padding (scan backwards for sentinel 128).
         */
        private byte[] extractFragmentBytes(ByteBuffer frame) {
            ByteBuffer b = Utils.byteBufferClone(frame);
            int limit = b.limit();
            int end = limit;
            for (int i = limit - 1; i >= Header.HEADER_SIZE; i--) {
                if (b.get(i) == PAD_SENTINEL) { end = i; break; }
            }
            int len = end - Header.HEADER_SIZE;
            if (len < 0) len = 0;
            byte[] out = new byte[len];
            for (int i = 0; i < len; i++) out[i] = b.get(Header.HEADER_SIZE + i);
            return out;
        }

        private void sendPayloadAck(PayloadHeader in) {
            int me = sourceId.get();
            int origSrc = in.getSourceId();
            int nh = routingTable.getOrDefault(origSrc, origSrc);
            PayloadHeader ack = new PayloadHeader(true, me, origSrc, in.isLastFragment(), nh, in.getSequenceNumber());
            ByteBuffer buf = ByteBuffer.allocate(FRAME_SIZE);
            buf.putShort((short) ack.assemble());
            buf.put(Header.HEADER_SIZE, PAD_SENTINEL);
            try { sendingQueue.put(new Message(MessageType.DATA, buf)); }
            catch (InterruptedException ignored) {}
        }
    }
}

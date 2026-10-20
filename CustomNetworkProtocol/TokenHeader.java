/**
 * Token packet header (MSB = 1, nonce in {0xF (pass), 0xE (ack)}).
 *
 * Byte 0: [1 | src(3) | nonce(4)]
 * Byte 1: [nextHop(3) | destId(3) | 00]
 */
public class TokenHeader implements Header {
    public static final int NONCE_PASS = 0xF;
    public static final int NONCE_ACK  = 0xE;

    private int sourceId;
    private int nonce;
    private int nextHop;
    private int destId;

    public TokenHeader(int sourceId, int nonce, int nextHop, int destId) {
        this.sourceId = sourceId;
        this.nonce = nonce;
        this.nextHop = nextHop;
        this.destId = destId;
    }

    @Override
    public int assemble() {
        int out = 0;
        out |= 1 << 15;
        out |= (sourceId & 0x07) << 12;
        out |= (nonce    & 0x0F) << 8;
        out |= (nextHop  & 0x07) << 5;
        out |= (destId   & 0x07) << 2;
        return out & 0xFFFF;
    }

    @Override
    public Header copy() {
        return new TokenHeader(sourceId, nonce, nextHop, destId);
    }

    @Override public boolean isAck() { return nonce == NONCE_ACK; }

    @Override public int getSourceId() { return sourceId; }
    @Override public void setSourceId(int sourceId) { this.sourceId = sourceId; }

    public int getNonce() { return nonce; }
    public void setNonce(int nonce) { this.nonce = nonce; }

    public int getNextHop() { return nextHop; }
    public void setNextHop(int nextHop) { this.nextHop = nextHop; }

    public int getDestId() { return destId; }
    public void setDestId(int destId) { this.destId = destId; }
}

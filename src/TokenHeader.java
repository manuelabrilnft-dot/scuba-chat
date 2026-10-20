public class TokenHeader implements Header {
    private int sourceId;
    private int destId;
    private int nextHop;
    private int nonce;

    public TokenHeader(int sourceId, int nonce, int nextHop, int destId) {
        this.sourceId = sourceId;
        this.destId = destId;
        this.nextHop = nextHop;
    }

    public void setDestId(int destId) {
        this.destId = destId;
    }

    public void setNextHop(int nextHop) {
        this.nextHop = nextHop;
    }

    public void setNonce(int nonce) {
        this.nonce = nonce;
    }

    public int getDestId() {
        return destId;
    }

    public int getNextHop() {
        return nextHop;
    }

    public int getNonce() {
        return nonce;
    }

    @Override
    public Header copy() {
        return new TokenHeader(sourceId, nonce, nextHop, destId);
    }

    @Override
    public int assemble() {
        int output = 0;
        output = (1 << 15) | output;
        output = (sourceId << 12) | output;
        output = (nonce << 8) | output;
        output = (nextHop << 5) | output;
        output = (destId << 2) | output;
        System.out.println("Passing Token: " + Integer.toBinaryString(output));
        return output;
    }

    @Override
    public boolean isAck() {
        return false;
    }

    @Override
    public int getSourceId() {
        return sourceId;
    }

    @Override
    public void setSourceId(int sourceId) {
        this.sourceId = sourceId;
    }
}

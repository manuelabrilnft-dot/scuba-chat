import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Utils {

    public interface TimeoutEvent {
        void onElapsed(Object data);
    }

    /**
     * Keyed timeout registry. {@link #setTimeout} with an existing key cancels
     * the previous timer. {@link #cancel} removes and interrupts.
     */
    public static class Timeout {
        private static final Map<String, Thread> active = new ConcurrentHashMap<>();

        public static void setTimeout(String key, Object data, long ms, TimeoutEvent ev) {
            cancel(key);
            Thread t = new Thread(() -> {
                try {
                    Thread.sleep(ms);
                    active.remove(key);
                    ev.onElapsed(data);
                } catch (InterruptedException ignored) {}
            }, "timeout-" + key);
            active.put(key, t);
            t.setDaemon(true);
            t.start();
        }

        public static void cancel(String key) {
            Thread t = active.remove(key);
            if (t != null) t.interrupt();
        }

        public static boolean has(String key) {
            return active.containsKey(key);
        }
    }

    public static ByteBuffer byteBufferClone(ByteBuffer original) {
        ByteBuffer clone = ByteBuffer.allocate(original.capacity());
        original.rewind();
        clone.put(original);
        original.rewind();
        clone.flip();
        return clone;
    }
}

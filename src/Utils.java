import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Utils {

    public interface TimeoutEvent {
        void onElapsed(Object data);
    }

    public static class Timeout {
        /*
            TODO: We need to create a class to handle the Timeouts for the messages in order to retransmit them
             -> We can create a function TimeoutElapsed() for when the timeout is over
             -> In the run() function we have a while loop with a boolean that is set to true on the SetTimeout
             -> We need a way to store the events for the different Messages
             -> Each Event should be an object that
         */

        private static final Map<String, Thread> activeTimeouts = new ConcurrentHashMap<>();

        public static void setTimeout(String key, Object data, long milliseconds, TimeoutEvent event) {
            // we need to stop any thread that may have the same key
            cancel(key);

            Thread timeout = new Thread(() -> {
                try {
                    Thread.sleep(milliseconds);

                    // When done, remove the event and call onElapsed of the event so we can execute new logic
                    activeTimeouts.remove(key);
                    event.onElapsed(data);
                } catch (InterruptedException e) {

                }
            });

            activeTimeouts.put(key, timeout);
            timeout.setDaemon(true);
            timeout.start();
        }

        public static void cancel(String key) {
            Thread current = activeTimeouts.remove(key);
            if (current != null) {
                current.interrupt();
            }
        }
    }

    public static ByteBuffer byteBufferClone(ByteBuffer original) {
        ByteBuffer clone = ByteBuffer.allocate(original.capacity());
        original.rewind();//copy from the beginning
        clone.put(original);
        original.rewind();
        clone.flip();
        return clone;
    }
}
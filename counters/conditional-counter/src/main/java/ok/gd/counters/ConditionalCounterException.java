package ok.gd.counters;

/**
 * Created by olegklymchuk on 8/23/16.
 *
 */

public class ConditionalCounterException extends Exception {

    public ConditionalCounterException(String message) {
        super(message);
    }

    public ConditionalCounterException(String message, Exception innerException) {
        super(message, innerException);
    }
}

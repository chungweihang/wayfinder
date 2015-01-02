package smallworld.util;

import java.math.BigInteger;
import java.util.concurrent.atomic.AtomicReference;

public final class AtomicBigInteger {

    private final AtomicReference<BigInteger> valueHolder = new AtomicReference<BigInteger>();

    public AtomicBigInteger(BigInteger bigInteger) {
        valueHolder.set(bigInteger);
    }

    public AtomicBigInteger() {
        valueHolder.set(BigInteger.ZERO);
    }

    public BigInteger incrementAndGet() {
        for (; ; ) {
            BigInteger current = valueHolder.get();
            BigInteger next = current.add(BigInteger.ONE);
            if (valueHolder.compareAndSet(current, next)) {
                return next;
            }
        }
    }
    
    public BigInteger addAndGet(long increment) {
        for (; ; ) {
            BigInteger current = valueHolder.get();
            BigInteger next = current.add(BigInteger.valueOf(increment));
            if (valueHolder.compareAndSet(current, next)) {
                return next;
            }
        }
    }
}
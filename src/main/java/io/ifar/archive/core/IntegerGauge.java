package io.ifar.archive.core;

import com.codahale.metrics.Gauge;
import java.util.concurrent.atomic.AtomicInteger;

public class IntegerGauge implements Gauge<Integer>
{
    private final AtomicInteger value = new AtomicInteger(0);

    public IntegerGauge() { }

    public void set(int d) {
        value.set(d);
    }

    public void inc() { value.incrementAndGet(); }

    public void dec() { value.decrementAndGet(); }

    @Override
    public Integer getValue() {
        return value.get();
    }
}


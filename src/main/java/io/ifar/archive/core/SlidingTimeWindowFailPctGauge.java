package io.ifar.archive.core;

import com.codahale.metrics.RatioGauge;
import com.codahale.metrics.SlidingTimeWindowReservoir;

import java.util.concurrent.TimeUnit;

public class SlidingTimeWindowFailPctGauge extends RatioGauge{
    private SlidingTimeWindowReservoir failed;
    private SlidingTimeWindowReservoir succeeded;

    private final long window;
    private final TimeUnit windowUnit;

    @Override
    protected Ratio getRatio() {
        int failed_size = failed.size();
        return Ratio.of(failed_size, failed_size+succeeded.size());
    }

    public SlidingTimeWindowFailPctGauge(long window, TimeUnit windowUnit) {
        this.failed = new SlidingTimeWindowReservoir(window, windowUnit);
        this.succeeded = new SlidingTimeWindowReservoir(window, windowUnit);
        this.window = window;
        this.windowUnit = windowUnit;
    }

    public void reset() {
        failed = new SlidingTimeWindowReservoir(window, windowUnit);
        succeeded = new SlidingTimeWindowReservoir(window, windowUnit);
    }

    public int getAndIncrementFailed() {
        int size = failed.size();
        failed.update(0);
        return size;
    }

    public int getAndIncrementSucceeded() {
        int size = succeeded.size();
        succeeded.update(0);
        return size;
    }

    public int getFailed() {
        return failed.size();
    }

    public int getSucceeded() {
        return succeeded.size();
    }
}

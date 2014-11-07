package io.ifar.archive.core;

class NamedThreadRunnable implements Runnable {
    private final Runnable runnable;
    private final String threadName;
    public NamedThreadRunnable(Runnable runnable, String threadName) {
        this.runnable = runnable;
        this.threadName = threadName;
    }
    @Override
    public void run() {
        String originalName = Thread.currentThread().getName();
        Thread.currentThread().setName(threadName);
        try {
            runnable.run();
        } finally {
            Thread.currentThread().setName(originalName);
        }
    }
}

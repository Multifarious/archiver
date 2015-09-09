package io.ifar.archive;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ErrorLimitConfiguration {

    @JsonProperty
    double errorLimitPercent = 5.0;

    @JsonProperty
    int triesBeforeEnableErrorLimit = 10;

    @JsonProperty
    int secondsToWaitOnReachErrorLimit = 10 * 60;

    public double getErrorLimitPercent() {
        return errorLimitPercent;
    }

    public int getTriesBeforeEnableErrorLimit() {
        return triesBeforeEnableErrorLimit;
    }

    public int getSecondsToWaitOnReachErrorLimit() {
        return secondsToWaitOnReachErrorLimit;
    }

}


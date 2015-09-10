package io.ifar.archive;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.util.Duration;

public class ErrorLimitConfiguration {

    @JsonProperty
    double errorLimitPercent = 5.0;

    @JsonProperty
    int triesBeforeEnableErrorLimit = 10;

    @JsonProperty
    Duration lengthToWaitOnReachErrorLimit = Duration.minutes(10);

    @JsonProperty
    Duration lengthOfErrorCheckingWindow = lengthToWaitOnReachErrorLimit;

    public double getErrorLimitPercent() {
        return errorLimitPercent;
    }

    public int getTriesBeforeEnableErrorLimit() {
        return triesBeforeEnableErrorLimit;
    }

    public Duration getlengthToWaitOnReachErrorLimit() {
        return lengthToWaitOnReachErrorLimit;
    }

    public Duration getlengthOfErrorCheckingWindow() {
        return lengthOfErrorCheckingWindow;
    }

}


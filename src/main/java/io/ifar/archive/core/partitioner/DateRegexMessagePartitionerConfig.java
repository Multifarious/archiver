package io.ifar.archive.core.partitioner;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;

public class DateRegexMessagePartitionerConfig {
    @JsonProperty
    @NotNull
    String dateExtractRegex;

    @JsonProperty
    String dateTimeFormatPattern;

    public String getDateExtractRegex() {
        return dateExtractRegex;
    }

    public void setDateExtractRegex(String dateExtractRegex) {
        this.dateExtractRegex = dateExtractRegex;
    }

    public String getDateTimeFormatPattern() {
        return dateTimeFormatPattern;
    }

    public void setDateTimeFormatPattern(String dateTimeFormatPattern) {
        this.dateTimeFormatPattern = dateTimeFormatPattern;
    }
}

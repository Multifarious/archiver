package io.ifar.archive.core.partitioner;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Partitions by Kafka partition number and a date/time extracted from the message with a regex.
 * If the regex fails to match, system time is used.
 */
public class DateRegexKafkaMessagePartitioner implements KafkaMessagePartitioner {

    private final Pattern dateExtractRegexPattern;
    private final DateTimeFormatter dateTimeFormatter;

    public DateRegexKafkaMessagePartitioner(String dateExtractRegex, String dateTimeFormatPattern) {
        this.dateExtractRegexPattern = Pattern.compile(dateExtractRegex);
        if(dateTimeFormatPattern != null) {
            this.dateTimeFormatter = DateTimeFormat.forPattern(dateTimeFormatPattern);
        } else {
            this.dateTimeFormatter = null;
        }
    }

    @Override
    public KafkaMessagePartitioner.ArchivePartitionData archivePartitionFor(String topic, int partition, byte[] rawMessagePayload) {
        String message = new String(rawMessagePayload);
        Date date = new Date();

        Matcher matcher = dateExtractRegexPattern.matcher(message);
        if(matcher.matches() && matcher.groupCount() > 0) {
            String dateStr = matcher.group(1);
            try {
                if(dateTimeFormatter != null) {
                    DateTime dt = dateTimeFormatter.parseDateTime(dateStr);
                    date = dt.toDate();
                } else {
                    date = new Date(Long.parseLong(dateStr));
                }
            } catch (IllegalArgumentException e) {
                // default to now
            }
        }

        return new KafkaMessagePartitioner.ArchivePartitionData(message, String.format("%d", partition), date);
    }
}

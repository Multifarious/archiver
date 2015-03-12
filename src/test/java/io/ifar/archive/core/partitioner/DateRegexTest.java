package io.ifar.archive.core.partitioner;

import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;

@RunWith(Parameterized.class)
public class DateRegexTest {

    private final String dateExtractRegex;
    private final String dateTimeFormatPattern;
    private final String message;
    private final Date expected;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(
            new Object[][] {
                    {"<[0-9]*>([^\\s]+).*", "yyyy-MM-dd'T'HH:mm:ssZZ", "<134>2014-08-26T21:10:36Z cache-sjc3134 kafkalog[24027]: 12.345.67.890 \"-\" \"-\" %s GET /w.gif/?bidid=1234&pr=2.02?auction_price=2.02&auction_id=foobar", DateTime.parse("2014-08-26T21:10:36Z").toDate()},
                    {".*\"ts\":([0-9]+).*", null, "{\"ts\":1426028626160, \"bid\":{}}", new Date(1426028626160L)},
                    {"([0-9]+):.*", null, "1426028626160: {foo:true}", new Date(1426028626160L)},
                    {".*\"millis\":([0-9]+).*", null, "{\"rec\":{\"millis\":1425670026685},\"w\":null,\"c\":null,\"a\":null}", new Date(1425670026685L)},
                    {".*\"time\":\"([^\"]+)\".*", "yyyy-MM-dd'T'HH:mm:ss.SSSZZ", "{\"id\":\"foo\",\"bidId\":\"bar\",\"success\":true,\"numRetries\":0,\"lastError\":null,\"time\":\"2015-03-05T21:39:46.252Z\"}", DateTime.parse("2015-03-05T21:39:46.252Z").toDate()}
            });
    }

    public DateRegexTest(String dateExtractRegex, String dateTimeFormatPattern, String message, Date expected) {
        this.dateExtractRegex = dateExtractRegex;
        this.dateTimeFormatPattern = dateTimeFormatPattern;
        this.message = message;
        this.expected = expected;
    }

    @Test
    public void testDateParsing() {
        DateRegexKafkaMessagePartitioner partitioner =
                new DateRegexKafkaMessagePartitioner(dateExtractRegex, dateTimeFormatPattern);
        ArchivePartitionData apd = partitioner.archivePartitionFor("topic", 0, message.getBytes());
        Assert.assertEquals(expected, apd.archiveTime);
    }
}

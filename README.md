# kafka-archiver
----

## Purpose

`kafka-archiver` scribes [Apache Kafka](http://kafka.apache.org/) topic(s) to S3.

## Configuration

TODO.

## Support / Contributing

Contributions may be proposed via a pull request.  (Please email a completed
[individual CLA](https://github.com/Multifarious/oss/blob/master/multifarious_cla.txt) or
[corporate CLA](https://github.com/Multifarious/oss/blob/master/multifarious_ccla.txt) to
[info@mult.ifario.us](mailto:info@mult.ifario.us), as appropriate).

### Maven Coordinates

Include the following in your `pom.xml`:

<pre>
&lt;dependency>
  &lt;groupId>io.ifar.kafka-archiver&lt;/groupId>
  &lt;artifactId>kafka-archiver&lt;/artifactId>
  &lt;version>2&lt;/version>
&lt;/dependency>
</pre>

But most likely, you'll just want to deploy the shaded JAR as you would any other
[Dropwizard](http://dropwizard.io) service.

### Snapshots

Snapshots are available from the [Sonatype OSS Snapshot Repository](https://oss.sonatype.org/content/repositories/snapshots/).

### Releases

Releases are available on Maven Central.

## License

The license for `kaffa-archiver` is [BSD 2-clause](http://opensource.org/licenses/BSD-2-Clause).  This information is also
present in the `LICENSE` file and in `pom.xml`.

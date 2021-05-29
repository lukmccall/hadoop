package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFS {
  private static final String NAME_NODE = "hdfs://localhost:9000";

  public static void main(String[] args) throws URISyntaxException, IOException {
    FileSystem fs = FileSystem.get(new URI(NAME_NODE), new Configuration());

    RemoteIterator<LocatedFileStatus> remoteIterator = fs.listFiles(new Path("/data"), true);
    while (remoteIterator.hasNext()) {
      LocatedFileStatus fileStatus = remoteIterator.next();

      System.out.println(fileStatus.toString());
    }

  }
}

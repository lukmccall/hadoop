package hadoop;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

public class HDFS {
  private static final String NAME_NODE = "hdfs://localhost:9000";

  public static void main(String[] args) throws IOException {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", NAME_NODE);
    conf.addResource(new Path("/etc/hadoop/core-site.xml"));
    conf.addResource(new Path("/etc/hadoop/hdfs-site.xml"));

    FileSystem fs = FileSystem.get(conf);

    RemoteIterator<LocatedFileStatus> remoteIterator = fs.listFiles(new Path("/data"), true);

    System.out.println("Files:");
    while (remoteIterator.hasNext()) {
      LocatedFileStatus fileStatus = remoteIterator.next();
      System.out.println(fileStatus.getPath().toString());
    }

    System.out.println("---------------");

    boolean exists = fs.exists(new Path("/data/f1.txt"));

    System.out.println("Check if f1.txt exists: " + exists);

    if (!exists) {
      return;
    }


    System.out.println("---------------");


    System.out.println("Create file:");
    String fileName = "java.txt";
    Path path = new Path("/data/" + fileName);
    boolean shouldRemoveFile = fs.exists(path);
    if (shouldRemoveFile) {
      fs.delete(path, true);
    }

    OutputStream os = fs.create(path, () -> System.out.println("..."));
    BufferedWriter br = new BufferedWriter( new OutputStreamWriter(os, StandardCharsets.UTF_8));
    br.write("Hello from java \n");
    br.write("👋 \n");
    br.close();

    System.out.println("---------------");

    System.out.println("Read file:");
    FSDataInputStream inputStream = fs.open(path);
    String content = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
    System.out.println(fileName + ":");
    System.out.println(content);
    inputStream.close();

    fs.close();
  }
}

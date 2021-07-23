package org.group15.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

public class CustomLock {

  FileChannel channel;

  FileLock lock;

  public boolean acquireLock(File file) {
    try {
      channel = new RandomAccessFile(file, "rw").getChannel();
      lock = channel.lock();
      return true;
    } catch (FileNotFoundException e) {
      return false;
    } catch (IOException e) {
      return false;
    }
  }

  public boolean releaseLock(File file) {
    try {
      this.channel = new RandomAccessFile(file, "rw").getChannel();
      this.lock.release();
      this.channel.close();
      return true;
    } catch (FileNotFoundException fileNotFoundException) {
      return false;
    } catch (IOException ioException) {
      return false;
    }
  }

}

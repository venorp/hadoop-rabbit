package org.apache.hadoop.fs.sftp;

public class SFTPException extends RuntimeException {

	  private static final long serialVersionUID = 1L;

	  public SFTPException(String message) {
	    super(message);
	  }

	  public SFTPException(Throwable t) {
	    super(t);
	  }

	  public SFTPException(String message, Throwable t) {
	    super(message, t);
	  }
}

package org.apache.hadoop.fs.sftp;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;

public class SFTPInputStream extends FSInputStream {

	private InputStream wrappedStream;
	
	private SFTPConnection conn;
	
	private FileSystem.Statistics stats;
	
	private boolean closed;
	
	private long pos;

	public SFTPInputStream(InputStream stream, SFTPConnection conn,
			FileSystem.Statistics stats) {
		if (stream == null) {
			throw new IllegalArgumentException("Null InputStream");
		}
		if (conn.getChannelSftp() == null || !conn.getChannelSftp().isConnected()) {
			throw new IllegalArgumentException(
					"SFTP client null or not connected");
		}
		this.wrappedStream = stream;
		this.conn = conn;
		this.stats = stats;
		this.pos = 0;
		this.closed = false;
	}

	public long getPos() throws IOException {
		return pos;
	}

	// We don't support seek.
	public void seek(long pos) throws IOException {
		throw new IOException("Seek not supported");
	}

	public boolean seekToNewSource(long targetPos) throws IOException {
		throw new IOException("Seek not supported");
	}

	public synchronized int read() throws IOException {
		if (closed) {
			throw new IOException("Stream closed");
		}

		int byteRead = wrappedStream.read();
		if (byteRead >= 0) {
			pos++;
		}
		if (stats != null & byteRead >= 0) {
			stats.incrementBytesRead(1);
		}
		return byteRead;
	}

	public synchronized int read(byte buf[], int off, int len)
			throws IOException {
		if (closed) {
			throw new IOException("Stream closed");
		}

		int result = wrappedStream.read(buf, off, len);
		if (result > 0) {
			pos += result;
		}
		if (stats != null & result > 0) {
			stats.incrementBytesRead(result);
		}

		return result;
	}

	public synchronized void close() throws IOException {
		if (closed) {
			throw new IOException("Stream closed");
		}
		super.close();
		closed = true;
		if (!conn.getChannelSftp().isConnected()) {
			throw new SFTPException("Client not connected");
		}

//		boolean cmdCompleted = client.completePendingCommand();
//		boolean cmdCompleted = client.isEOF();
//		conn.getChannelSftp().disconnect();
//		conn.getSession().disconnect();
		if(null != conn){
			conn.disconnect();
		}
	/*	if (!cmdCompleted) {
			throw new SFTPException("Could not complete transfer");
		}*/
	}

	// Not supported.

	public boolean markSupported() {
		return false;
	}

	public void mark(int readLimit) {
		// Do nothing
	}

	public void reset() throws IOException {
		throw new IOException("Mark not supported");
	}

}

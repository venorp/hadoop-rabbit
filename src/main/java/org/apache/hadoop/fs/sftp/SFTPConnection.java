package org.apache.hadoop.fs.sftp;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.Session;

public class SFTPConnection {

	private final Session session;

	private final ChannelSftp channelSftp;

	public SFTPConnection(Session session, ChannelSftp channelSftp) {
		super();
		this.session = session;
		this.channelSftp = channelSftp;
	}

	public Session getSession() {
		return session;
	}

	public ChannelSftp getChannelSftp() {
		return channelSftp;
	}

	public void disconnect() {
		if (false == this.getSession().isConnected()) {
			throw new SFTPException("Client not connected");
		}
		if (null != this.getChannelSftp()) {
			this.getChannelSftp().disconnect();
		}

		if (null != this.getSession()) {
			this.getSession().disconnect();
		}
	}

}

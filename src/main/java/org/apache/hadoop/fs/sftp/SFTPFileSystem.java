package org.apache.hadoop.fs.sftp;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ftp.FTPException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;

public class SFTPFileSystem extends FileSystem {
	
	private static final Logger LOG = LoggerFactory.getLogger(SFTPFileSystem.class);

	public static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;

	public static final int DEFAULT_BLOCK_SIZE = 4 * 1024;
	
	public static final int DEFAULT_SFTP_TIMEOUT = 10*1000; 
	
	private URI uri;

	@Override
	public void initialize(URI uri, Configuration conf) throws IOException { // get
		super.initialize(uri, conf);
		// get host information from uri (overrides info in conf)
		String host = uri.getHost();
		host = (host == null) ? conf.get("fs.sftp.host", null) : host;
		if (host == null) {
			throw new IOException("Invalid host specified");
		}
		conf.set("fs.sftp.host", host);
		// get port information from uri, (overrides info in conf)
		int port = uri.getPort();
		port = (port == -1) ? SFTPConstants.DEFAULT_PORT : port;
		conf.setInt("fs.sftp.host.port", port);

		// get user/password information from URI (overrides info in conf)
		String userAndPassword = uri.getUserInfo();
		if (userAndPassword == null) {
			userAndPassword = (conf.get("fs.sftp.user." + host, null) + ":" + conf
					.get("fs.sftp.password." + host, null));
			if (userAndPassword.equals("null:null")) {
				throw new IOException("Invalid user/passsword specified");
			}
		}
		String[] userPasswdInfo = userAndPassword.split(":");
		conf.set("fs.sftp.user." + host, userPasswdInfo[0]);
		if (userPasswdInfo.length > 1) {
			conf.set("fs.sftp.password." + host, userPasswdInfo[1]);
		} else {
			conf.set("fs.sftp.password." + host, null);
		}
		setConf(conf);
		this.uri = uri;
	}

	/**
	 * Connect to the SFTP server using configuration parameters *
	 * 
	 * @return An SFTPConnection instance
	 * @throws IOException
	 */
	private SFTPConnection connect() throws IOException {
		JSch jsch = null;
		Configuration conf = getConf();
		String host = conf.get("fs.sftp.host");
		int port = conf.getInt("fs.sftp.host.port", SFTPConstants.DEFAULT_PORT);
		String user = conf.get("fs.sftp.user." + host);
		String password = conf.get("fs.sftp.password." + host);
		jsch = new JSch();
		Session session = null;
		ChannelSftp sftpChaneel = null;
		try {
			session = jsch.getSession(user, host, port);
			session.setPassword(password);
			Properties config = new Properties();
		    config.put("StrictHostKeyChecking","no");
			session.setConfig(config);
			session.setTimeout(DEFAULT_SFTP_TIMEOUT);
			session.connect();
			Channel channel = session.openChannel("sftp");
			channel.connect();
			sftpChaneel = (ChannelSftp)channel;
		} catch (JSchException e) {
			LOG.error(e.getMessage());
			throw new IOException("Server - " + host
					+ " refused connection on port - " + port + ","+e.getMessage());
			
		}
		return new SFTPConnection(session, sftpChaneel);
	}

	/**
	 * Logout and disconnect the given SFTPConnection. *
	 * 
	 * @param client
	 * @throws IOException
	 */
	private void disconnect(SFTPConnection conn) throws IOException {
		if(null != conn){
			conn.disconnect();
		}
	}

	/**
	 * Resolve against given working directory. *
	 * 
	 * @param workDir
	 * @param path
	 * @return
	 */
	private Path makeAbsolute(Path workDir, Path path) {
		if (path.isAbsolute()) {
			return path;
		}
		return new Path(workDir, path);
	}

	@Override
	public FSDataInputStream open(Path file, int bufferSize) throws IOException {
		SFTPConnection conn = connect();
		ChannelSftp client = conn.getChannelSftp();
		Path workDir = null;;
		try {
			workDir = new Path(client.pwd());
		} catch (SftpException e1) {
			disconnect(conn);
			throw new IOException("Path " + file + " can not get current working directory.");
		}
		Path absolute = makeAbsolute(workDir, file);
		FileStatus fileStat = getFileStatus(conn.getChannelSftp(), absolute);
		if (fileStat.isDir()) {
			disconnect(conn);
			throw new IOException("Path " + file + " is a directory.");
		}
		InputStream in = null;
		FSDataInputStream fis = null;
		try{
			in = client.get(absolute.toUri().getPath().toString());
			fis = new FSDataInputStream(new SFTPInputStream(in, conn, statistics));
		}catch (Exception e) {
			disconnect(conn);
			if(null != fis){
				fis.close();
			}
			throw new IOException("Unable to open file: " + file + ", Aborting");
		}
		return fis;
	}

	/**
	 * A stream obtained via this call must be closed before using other APIs of
	 * this class or else the invocation will block.
	 */
	@Override
	public FSDataOutputStream create(final Path file, FsPermission permission,
			boolean overwrite, int bufferSize, short replication,
			long blockSize, Progressable progress) throws IOException {
//		final FTPClient client = connect();
		final SFTPConnection conn = connect();
		final ChannelSftp client = conn.getChannelSftp();
		Path workDir;
		try {
			workDir = new Path(client.pwd());
		} catch (SftpException e) {
			throw new IOException(e);
		}
		Path absolute = makeAbsolute(workDir, file);
		if (true == exists(client, file)) {
			if (true == overwrite) {
				try {
					delete(client, file);
				} catch (SftpException e) {
					throw new IOException(e);
				}
			} else {
				disconnect(conn);
				throw new IOException("File already exists: " + file);
			}
		}
		Path parent = absolute.getParent();
		try {
			if (parent == null
					|| !mkdirs(client, parent, FsPermission.getDefault())) {
				parent = (parent == null) ? new Path("/") : parent;
				disconnect(conn);
				throw new IOException("create(): Mkdirs failed to create: "
						+ parent);
			}
		} catch (SftpException e) {
			throw new IOException(e);
		}
		FSDataOutputStream fos = null;
		try {
			client.cd(parent.toUri().getPath().toString());
			
			fos = new FSDataOutputStream(client.put(file.getName()),statistics){

				@Override
				public void close() throws IOException {
					super.close();
					if (!client.isConnected()) {
						throw new SFTPException("Client not connected");
					}
//					boolean cmdCompleted = client.isEOF();
					disconnect(conn);
					/*if (!cmdCompleted) {
						throw new SFTPException(
								"Could not complete transfer file "+ file.getName());
					}*/
				}
				
			};
		} catch (SftpException e) {
			throw new IOException(e);
		}
		return fos;
	}

	/** This optional operation is not yet supported. */
	public FSDataOutputStream append(Path f, int bufferSize,
			Progressable progress) throws IOException {
		throw new IOException("Not supported");
	}

	/**
	 * Convenience method, so that we don't open a new connection when using
	 * this method from within another method. Otherwise every API invocation
	 * incurs the overhead of opening/closing a TCP connection.
	 */
	private boolean exists(ChannelSftp client, Path file) {
		try {
			return client.lstat(file.toUri().getPath()) != null;
		} catch (SftpException e) {
			return false;
		}
	}

	/** @deprecated Use delete(Path, boolean) instead */
	@Override
	@Deprecated
	public boolean delete(Path file) throws IOException {
		return delete(file, false);
	}

	@Override
	public boolean delete(Path file, boolean recursive) throws IOException {
		SFTPConnection conn = connect();
		ChannelSftp client = conn.getChannelSftp();
		try {
			boolean success = delete(client, file, recursive);
			return success;
		} catch (SftpException e) {
			throw new RuntimeException(e);
		} finally {
			disconnect(conn);
		}
	}

	/** @throws SftpException 
	 * @deprecated Use delete(Path, boolean) instead */
	@Deprecated
	private boolean delete(ChannelSftp client, Path file) throws IOException, SftpException {
		return delete(client, file, false);
	}

	/**
	 * Convenience method, so that we don't open a new connection when using
	 * this method from within another method. Otherwise every API invocation
	 * incurs the overhead of opening/closing a TCP connection.
	 * @throws SftpException 
	 */
	private boolean delete(ChannelSftp client, Path file, boolean recursive)
			throws IOException, SftpException {
		Path workDir = new Path(client.pwd());
		Path absolute = makeAbsolute(workDir, file);
		String pathName = absolute.toUri().getPath();
		FileStatus fileStat = getFileStatus(client, absolute);
		if (!fileStat.isDir()) {
			client.rm(pathName);
			return true;
		}
		FileStatus[] dirEntries = listStatus(client, absolute);
		if (dirEntries != null && dirEntries.length > 0 && !(recursive)) {
			throw new IOException("Directory: " + file + " is not empty.");
		}
		if (dirEntries != null) {
			for (int i = 0; i < dirEntries.length; i++) {
				delete(client, new Path(absolute, dirEntries[i].getPath()),
						recursive);
			}
		}
		client.rmdir(pathName);
		return true;
	}

	private FsAction getFsAction(String accessGroup) {
		FsAction action = FsAction.NONE;
		for(int i = 0; i < accessGroup.length();i++){
			switch (accessGroup.charAt(i)) {
			case 'r':
				action.or(FsAction.READ);
				break;
			case 'w':
				action.or(FsAction.WRITE);
				break;
			case 'x':
				action.or(FsAction.EXECUTE);
				break;
			default:
				break;
			}
		}
		return action;
	}

	private FsPermission getPermissions(SftpATTRS attr) {
		FsAction user, group, others;
		String perms = attr.getPermissionsString();
		user = getFsAction(perms.substring(1, 4));
		group = getFsAction(perms.substring(4, 7));
		others = getFsAction(perms.substring(7));
		return new FsPermission(user, group, others);
	}

	@Override
	public URI getUri() {
		return uri;
	}

	@Override
	public FileStatus[] listStatus(Path file) throws IOException {
		SFTPConnection conn = connect();
		ChannelSftp client = conn.getChannelSftp();
		FileStatus[] stats = null;
		try {
			try {
				stats = listStatus(client, file);
			} catch (SftpException e) {
				throw new IOException(e);
			}
		} finally {
			disconnect(conn);
		}
		return stats;
	}

	/**
	 * Convenience method, so that we don't open a new connection when using
	 * this method from within another method. Otherwise every API invocation
	 * incurs the overhead of opening/closing a TCP connection.
	 * @throws SftpException 
	 */
	private FileStatus[] listStatus(ChannelSftp client, Path file)
			throws IOException, SftpException {
		Path workDir = new Path(client.pwd());
		Path absolute = makeAbsolute(workDir, file);
		FileStatus fileStat = getFileStatus(client, absolute);
		if (!fileStat.isDir()) {
			return new FileStatus[] { fileStat };
		}
		@SuppressWarnings("unchecked")
		Vector<LsEntry> sftpFiles = client.ls(absolute.toUri().getPath().toString());
		List<LsEntry> preProcessEntry = new ArrayList<LsEntry>();
		for(int i = 0; i < sftpFiles.size();i++){
			boolean excludeDir  = sftpFiles.get(i).getFilename().endsWith(".");
//			boolean excludeHiddenFiles =  sftpFiles.get(i).getFilename().startsWith(".");
			if(false == excludeDir){
				preProcessEntry.add(sftpFiles.get(i));
			}
		}
		FileStatus[] fileStatusArray = new FileStatus[preProcessEntry.size()];
		for (int i = 0; i < preProcessEntry.size(); i++) {
			FileStatus fileStatus = getFileStatus(preProcessEntry.get(i), absolute);
//			System.out.println(fileStatus.getPath().getName());
//			String filename = fileStatus.getPath().getName();
//			if((false == ".".equals(filename)) || (false == ("..").equals(filename))){
			fileStatusArray[i] = fileStatus;
//			}
		
		}
		return fileStatusArray;
	}

	@Override
	public FileStatus getFileStatus(Path file) throws IOException {
		SFTPConnection conn = connect();
		ChannelSftp client = conn.getChannelSftp();
		try {
			FileStatus status = getFileStatus(client, file);
			return status;
		} finally {
			disconnect(conn);
		}
	}

	/**
	 * Convenience method, so that we don't open a new connection when using
	 * this method from within another method. Otherwise every API invocation
	 * incurs the overhead of opening/closing a TCP connection.
	 */
	@SuppressWarnings("unchecked")
	private FileStatus getFileStatus(ChannelSftp client, Path file)
			throws IOException {
		FileStatus fileStat = null;
		Path workDir = null;
		try {
			workDir = new Path(client.pwd());
		} catch (SftpException e1) {
			throw new SFTPException(e1.getMessage());
		}
		Path absolute = makeAbsolute(workDir, file);
		Path parentPath = absolute.getParent();
		if (parentPath == null) { // root dir
			long length = -1; // Length of root dir on server not known
			boolean isDir = true;
			int blockReplication = 1;
			long blockSize = DEFAULT_BLOCK_SIZE; // Block Size not known.
			long modTime = -1; // Modification time of root dir not known.
			Path root = new Path("/");
			return new FileStatus(length, isDir, blockReplication, blockSize,
					modTime, root.makeQualified(this));
		}
		String pathName = parentPath.toUri().getPath();
		Vector<LsEntry> entries = null;
		try {
			entries = client.ls(pathName);
			if(null != entries){
				for(int i = 0; i < entries.size();i++){
					if(entries.get(i).getFilename().equals(file.getName())){ // file found in dir
						fileStat = getFileStatus(entries.get(i),parentPath);
						break;
					}
				}
				if (fileStat == null) {
					throw new FileNotFoundException("File " + file
							+ " does not exist.");
				}
			} else {
				throw new FileNotFoundException("File " + file + " does not exist.");
			}
		} catch (SftpException e) {
			throw new SFTPException(e.getMessage());
		}
		return fileStat;
	}

	/**
	 * Convert the file information in LsEntry to a {@link FileStatus} object. *
	 * 
	 * @param ftpFile
	 * @param parentPath
	 * @return FileStatus
	 */
	private FileStatus getFileStatus(LsEntry entry, Path parentPath) {
		SftpATTRS attr = entry.getAttrs();
		long length = attr.getSize();
		boolean isDir = attr.isDir();
		int blockReplication = 1;
		long blockSize = DEFAULT_BLOCK_SIZE;
		long modTime = attr.getMTime();
		long accessTime = attr.getATime();
		FsPermission permission = getPermissions(attr);
		
		String user = String.valueOf(attr.getUId());
		String group = String.valueOf(attr.getGId());
		Path filePath = new Path(parentPath,entry.getFilename());
		return new FileStatus(length, isDir, blockReplication, blockSize,
				modTime, accessTime, permission, user, group,
				filePath.makeQualified(this));
	}

	@Override
	public boolean mkdirs(Path file, FsPermission permission)
			throws IOException {
		SFTPConnection conn = connect();
		ChannelSftp client = conn.getChannelSftp();
		try {
			boolean success;
			try {
				success = mkdirs(client, file, permission);
			} catch (SftpException e) {
				throw new IOException(e);
			}
			return success;
		} finally {
			disconnect(conn);
		}
	}

	/**
	 * Convenience method, so that we don't open a new connection when using
	 * this method from within another method. Otherwise every API invocation
	 * incurs the overhead of opening/closing a TCP connection.
	 * @throws SftpException 
	 */
	private boolean mkdirs(ChannelSftp client, Path file, FsPermission permission)
			throws IOException, SftpException {
		boolean created = true;
		Path workDir = new Path(client.pwd());
		Path absolute = makeAbsolute(workDir, file);
		String pathName = absolute.getName();
		if (false == exists(client, absolute)) {
			Path parent = absolute.getParent();
			created = (parent == null || mkdirs(client, parent,
					FsPermission.getDefault()));
			if (true == created) {
				String parentDir = parent.toUri().getPath();
				try{
					client.cd(parentDir);
					client.mkdir(pathName);
					created = true ;
				}catch (SftpException e) {
					created = false;
					throw new IOException(e);
				}
			}
		} else if (true == isFile(client, absolute)) {
			throw new IOException(String.format(
					"Can't make directory for path %s since it is a file.",
					absolute));
		}
		return created;
	}

	/**
	 * Convenience method, so that we don't open a new connection when using
	 * this method from within another method. Otherwise every API invocation
	 * incurs the overhead of opening/closing a TCP connection.
	 */
	private boolean isFile(ChannelSftp client, Path file) {
		try {
			return !getFileStatus(client, file).isDir();
		} catch (FileNotFoundException e) {
			return false; // file does not exist
		} catch (IOException ioe) {
			throw new FTPException("File check failed", ioe);
		}
	}

	/*
	 * Assuming that parent of both source and destination is the same. Is the
	 * assumption correct or it is suppose to work like 'move' ?
	 */
	@Override
	public boolean rename(Path src, Path dst) throws IOException {
		SFTPConnection conn = connect();
		ChannelSftp client = conn.getChannelSftp();
		try {
			boolean success = rename(client, src, dst);
			return success;
		} finally {
			disconnect(conn);
		}
	}

	/**
	 * Convenience method, so that we don't open a new connection when using
	 * this method from within another method. Otherwise every API invocation
	 * incurs the overhead of opening/closing a TCP connection.
	 * 
	 * @param client
	 * @param src
	 * @param dst
	 * @return
	 * @throws IOException
	 */
	private boolean rename(ChannelSftp client, Path src, Path dst)
			throws IOException {
		Path workDir;
		try {
			workDir = new Path(client.pwd());
		} catch (SftpException e) {
			throw new IOException(e);
		}
		Path absoluteSrc = makeAbsolute(workDir, src);
		Path absoluteDst = makeAbsolute(workDir, dst);
		if (!exists(client, absoluteSrc)) {
			throw new IOException("Source path " + src + " does not exist");
		}
		if (exists(client, absoluteDst)) {
			throw new IOException("Destination path " + dst
					+ " already exist, cannot rename!");
		}
		String parentSrc = absoluteSrc.getParent().toUri().toString();
		String parentDst = absoluteDst.getParent().toUri().toString();
		String from = src.getName();
		String to = dst.getName();
		if (!parentSrc.equals(parentDst)) {
			throw new IOException("Cannot rename parent(source): " + parentSrc
					+ ", parent(destination):  " + parentDst);
		}
		try {
			client.cd(parentSrc);
		} catch (SftpException e) {
			throw new IOException(e);
		}
		boolean renamed = true; 
		try {
			client.rename(from, to);
		} catch (SftpException e) {
			renamed = false;
		}
		return renamed;
	}

	@Override
	public Path getWorkingDirectory() {
		// Return home directory always since we do not maintain state.
		return getHomeDirectory();
	}

	@Override
	public Path getHomeDirectory() {
		SFTPConnection conn;
		try {
			conn = connect();
		} catch (IOException e1) {
			throw new  RuntimeException(e1);
		}
		ChannelSftp client = conn.getChannelSftp();
		try {
			Path homeDir = new Path(client.pwd());
			return homeDir;
		} catch (SftpException e) {
			throw new SFTPException("Failed to get home directory", e);
		} finally {
			try {
				disconnect(conn);
			} catch (IOException e) {
				throw new SFTPException("Failed to disconnect", e);
			}
		}
	}

	@Override
	public void setWorkingDirectory(Path newDir) {
		// we do not maintain the working directory state
	}
}

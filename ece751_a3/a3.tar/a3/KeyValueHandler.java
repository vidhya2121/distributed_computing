import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.log4j.Logger;
import org.apache.curator.framework.*;
import org.apache.curator.framework.api.CuratorWatcher;

public class KeyValueHandler implements KeyValueService.Iface, CuratorWatcher {
	private Map<String, String> myMap;
	private CuratorFramework curClient;
	private String zkNode;
	private String host;
	private int port;
	private ReadWriteLock lock;
	volatile InetSocketAddress primaryAddress;
	private boolean isPrimary;
	private KeyValueService.Client backUpClient;
	volatile private int childSize; 
//	Lock readLock;
	Lock writeLock;
	ReentrantLock backupCopyLock = new ReentrantLock();
	Logger log;

	public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) throws Exception {
		this.host = host;
		this.port = port;
		log = Logger.getLogger(KeyValueHandler.class.getName());
		this.isPrimary = false;
		this.curClient = curClient;
		this.zkNode = zkNode;
		this.curClient.sync();
		curClient.getChildren().usingWatcher(this).forPath(zkNode);
		myMap = new ConcurrentHashMap<String, String>();
		lock = new ReentrantReadWriteLock();
//		readLock = lock.readLock();
		writeLock = lock.writeLock();
		this.backUpClient = null;
	}

	public String get(String key) throws org.apache.thrift.TException {
		if (isPrimary == false) {
			throw new org.apache.thrift.TException("backup shouldn't read");
		}
		try {
//			readLock.lock();
			String ret = myMap.get(key);
//			log.info("getting value from server for key " + key);
			if (ret == null)
				return "";
			else
				return ret;
		} catch (Exception e) {
			throw new org.apache.thrift.TException("primary unable to read");
		}
//		finally {
//		
//			readLock.unlock();
//		}

	}

	public void put(String key, String value) throws org.apache.thrift.TException {
		if (isPrimary == false) {
			throw new org.apache.thrift.TException("backup shouldn't write");
		}
		try {
			writeLock.lock();
//			log.info("writing to server");
			myMap.put(key, value);
		} catch (Exception e) {
			throw new org.apache.thrift.TException("primary unable write");
		}
		try {
			
//			log.info("writing to backup");
			if (this.backUpClient == null) {
				if (childSize < 2) {
					return;
				}
				curClient.sync();
				List<String> children = curClient.getChildren().usingWatcher(this).forPath(zkNode);

				if (children.size() < 2) {
					return;
				}

				if (children.size() > 2) {
					log.info("print the size + " + children.size());
				}
				Collections.sort(children);
				byte[] data = curClient.getData().usingWatcher(this)
						.forPath(zkNode + "/" + children.get(children.size() - 1));
				String strData = new String(data);
				String[] backup = strData.split(":");
				TTransport transport = null;
				TSocket sock = new TSocket(backup[0], Integer.parseInt(backup[1]));
				transport = new TFramedTransport(sock);
				transport.open();
				TProtocol protocol = new TBinaryProtocol(transport);
				KeyValueService.Client client = new KeyValueService.Client(protocol);
				client.putBackup(key, value);
				log.info("success put");
				this.backUpClient = client;

			} else {
				this.backUpClient.putBackup(key, value);
			}

		} catch (Exception e) {
//			e.printStackTrace();
			this.backUpClient = null;
			throw new org.apache.thrift.TException("backup can't write");
		} finally {
			writeLock.unlock();
		}

		// put in backup as well

	}

	public void putBackup(String key, String value) throws org.apache.thrift.TException {
		try {
			while (backupCopyLock.isLocked()) {

			}
			writeLock.lock();
//			log.info("write in backup");
			myMap.put(key, value);
		} finally {
			writeLock.unlock();
		}
	}

	public void fullCopyFromPrimaryToBackup() throws org.apache.thrift.TException {
		TTransport transport = null;
		try {
			primaryAddress = getPrimary();
//			log.info("found primary as + " + primaryAddress.getPort());
			TSocket sock = new TSocket(primaryAddress.getHostName(), primaryAddress.getPort());
			transport = new TFramedTransport(sock, 1000000000);
			
			transport.open();
			TProtocol protocol = new TBinaryProtocol(transport);
			KeyValueService.Client client = new KeyValueService.Client(protocol);
//			this.writeLock.lock();
			backupCopyLock.lock();
			this.myMap = client.getMap();
			log.info("size : " + myMap.size());
		} catch (Exception e) {
			e.printStackTrace();
			throw new org.apache.thrift.TException("backup didn't copy all");
		} finally {
			backupCopyLock.unlock();
			transport.close();
//			this.writeLock.unlock();
		}

	}

	public Map<String, String> getMap() {
//		try {
//			readLock.lock();
		return myMap;
//		}
//		finally {
//			readLock.unlock();
//        }

	}

	public void setPrimary(boolean primary) {
		this.isPrimary = primary;
	}

	synchronized public void process(WatchedEvent event) {
		log.info("ZooKeeper event " + event);
		log.info("in process");
		try {
			primaryAddress = getPrimary();
		} catch (Exception e) {
			log.error("Unable to determine primary");
		}
	}

	InetSocketAddress getPrimary() throws Exception {
		while (true) {
			curClient.sync();
			List<String> children = curClient.getChildren().usingWatcher(this).forPath(zkNode);
			childSize = children.size();
			if (children.size() == 0) {
				log.error("No primary found");
				Thread.sleep(100);
				continue;
			}
			Collections.sort(children);
			byte[] data = curClient.getData().usingWatcher(this).forPath(zkNode + "/" + children.get(0));
			String strData = new String(data);
			String[] primary = strData.split(":");
			log.info("Found primary " + strData);
			log.info("child size +" + children.size());
			childSize = children.size();
			if (this.host.equals(primary[0]) && this.port == Integer.parseInt(primary[1])) {
				log.info("setting primary");
				this.isPrimary = true;
			}
			return new InetSocketAddress(primary[0], Integer.parseInt(primary[1]));
		}
	}
}

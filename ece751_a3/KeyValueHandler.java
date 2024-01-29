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
			while (backupCopyLock.isLocked()) {

			}
			writeLock.lock();
//			log.info("writing to server");
			myMap.put(key, value);
		} catch (Exception e) {
			throw new org.apache.thrift.TException("primary unable write");
		}
		try {

//			log.info("writing to backup");
			if (this.backUpClient != null) {
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
			writeLock.lock();
//			log.info("write in backup");
			myMap.put(key, value);
		} finally {
			writeLock.unlock();
		}
	}

	public void fullCopyFromPrimaryToBackup(String host, int port) throws org.apache.thrift.TException {
		try {
			backupCopyLock.lock();
			ExecutorService exec = Executors.newFixedThreadPool(10);
			List<String> keyList = new ArrayList(this.myMap.keySet());
			List<String> valueList = new ArrayList(this.myMap.values());
			log.info("to copy + " + this.myMap.size());
			int testSize = 100000;
			int endSize = testSize;
			int startSize = 0;
			while (endSize < keyList.size()) {
				final int finslStart = startSize;
				final int finalEnd = endSize;
				exec.execute(new Runnable() {
					@Override
					public void run() {
						TTransport transport = null;
						try {
							TSocket sock = new TSocket(host, port);
							transport = new TFramedTransport(sock);
							transport.open();
							TProtocol protocol = new TBinaryProtocol(transport);
							KeyValueService.Client client = new KeyValueService.Client(protocol);
							log.info("copying + " + finslStart + " to " + finalEnd);
							client.putMap(keyList.subList(finslStart, finalEnd),
									valueList.subList(finslStart, finalEnd));
						} catch (Exception e) {

						} finally {
							transport.close();
						}

					}
				});
				startSize = endSize;
				endSize = endSize + testSize;
			}
			if (startSize < keyList.size()) {
				final int finslStart = startSize;
				exec.execute(new Runnable() {
					@Override
					public void run() {
						TTransport transport = null;
						try {
							TSocket sock = new TSocket(host, port);
							transport = new TFramedTransport(sock);
							transport.open();
							TProtocol protocol = new TBinaryProtocol(transport);
							KeyValueService.Client client = new KeyValueService.Client(protocol);
							log.info("copying + " + finslStart + " to " + keyList.size());
							client.putMap(keyList.subList(finslStart, keyList.size()),
									valueList.subList(finslStart, keyList.size()));
						} catch (Exception e) {

						} finally {
							transport.close();
						}

					}
				});
			}
			TTransport transport = null;
			TSocket sock = new TSocket(host, port);
			transport = new TFramedTransport(sock);
			transport.open();
			TProtocol protocol = new TBinaryProtocol(transport);
			KeyValueService.Client client = new KeyValueService.Client(protocol);
			this.backUpClient = client;
			log.info("fully copied");
			exec.shutdown();
			try {
				exec.awaitTermination(1, TimeUnit.DAYS);
			} catch (InterruptedException ex) {
			}

		} catch (Exception e) {
			e.printStackTrace();
			throw new org.apache.thrift.TException("backup didn't copy all");
		} finally {
			backupCopyLock.unlock();
//			this.writeLock.unlock();
		}

	}

	public void putMap(List<String> keys, List<String> values) {
		for (int i = 0; i < keys.size(); i++) {
			this.myMap.put(keys.get(i), values.get(i));
		}
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
		curClient.sync();
		List<String> children = curClient.getChildren().usingWatcher(this).forPath(zkNode);
		Collections.sort(children);
		byte[] data = curClient.getData().usingWatcher(this).forPath(zkNode + "/" + children.get(0));
		String strData = new String(data);
		String[] primary = strData.split(":");
		log.info("Found primary " + strData);
		log.info("child size +" + children.size());
		if (this.host.equals(primary[0]) && this.port == Integer.parseInt(primary[1])) {
			log.info("setting primary");
			this.isPrimary = true;
			if (children.size() > 1) {
				byte[] data1 = curClient.getData().usingWatcher(this)
						.forPath(zkNode + "/" + children.get(children.size() - 1));
				String strData1 = new String(data1);
				String[] backup = strData1.split(":");
				fullCopyFromPrimaryToBackup(backup[0], Integer.parseInt(backup[1]));
			}
		}
		return new InetSocketAddress(primary[0], Integer.parseInt(primary[1]));

	}

	/*
	 * public void fullCopyFromPrimaryToBackup(String host, int port) throws
	 * org.apache.thrift.TException { try { backupCopyLock.lock(); ExecutorService
	 * exec = Executors.newFixedThreadPool(4); List<String> keyList = new
	 * ArrayList(this.myMap.keySet()); List<String> valueList = new
	 * ArrayList(this.myMap.values()); int fourth = (int) keyList.size() / 4;
	 * exec.execute(new Runnable() {
	 * 
	 * @Override public void run() { TTransport transport = null;
	 * 
	 * try { TSocket sock = new TSocket(host, port); transport = new
	 * TFramedTransport(sock, 1000000000); transport.open(); TProtocol protocol =
	 * new TBinaryProtocol(transport); KeyValueService.Client client = new
	 * KeyValueService.Client(protocol); log.info("copying first half");
	 * client.putMap(keyList.subList(0, fourth), valueList.subList(0, fourth)); }
	 * catch (Exception e) {
	 * 
	 * } finally { transport.close(); }
	 * 
	 * } }); ; exec.execute(new Runnable() {
	 * 
	 * @Override public void run() { TTransport transport = null; try { TSocket sock
	 * = new TSocket(host, port); transport = new TFramedTransport(sock,
	 * 1000000000); transport.open(); TProtocol protocol = new
	 * TBinaryProtocol(transport); KeyValueService.Client client = new
	 * KeyValueService.Client(protocol); log.info("copying secod half");
	 * client.putMap(keyList.subList(fourth, fourth*2), valueList.subList(fourth,
	 * fourth*2)); } catch (Exception e) {
	 * 
	 * } finally { transport.close(); }
	 * 
	 * } });
	 * 
	 * exec.execute(new Runnable() {
	 * 
	 * @Override public void run() { TTransport transport = null; try { TSocket sock
	 * = new TSocket(host, port); transport = new TFramedTransport(sock,
	 * 1000000000); transport.open(); TProtocol protocol = new
	 * TBinaryProtocol(transport); KeyValueService.Client client = new
	 * KeyValueService.Client(protocol); log.info("copying third half");
	 * client.putMap(keyList.subList(fourth*2, fourth*3),
	 * valueList.subList(fourth*2, fourth*3)); } catch (Exception e) {
	 * 
	 * } finally { transport.close(); }
	 * 
	 * } });
	 * 
	 * exec.execute(new Runnable() {
	 * 
	 * @Override public void run() { TTransport transport = null; try { TSocket sock
	 * = new TSocket(host, port); transport = new TFramedTransport(sock,
	 * 1000000000); transport.open(); TProtocol protocol = new
	 * TBinaryProtocol(transport); KeyValueService.Client client = new
	 * KeyValueService.Client(protocol); log.info("copying fourth half");
	 * client.putMap(keyList.subList(fourth*3, keyList.size()),
	 * valueList.subList(fourth*3, keyList.size())); } catch (Exception e) {
	 * 
	 * } finally { transport.close(); }
	 * 
	 * } }); TTransport transport = null; TSocket sock = new TSocket(host, port);
	 * transport = new TFramedTransport(sock, 1000000000); transport.open();
	 * TProtocol protocol = new TBinaryProtocol(transport); KeyValueService.Client
	 * client = new KeyValueService.Client(protocol); this.backUpClient = client;
	 * log.info("fully copied"); exec.shutdown(); try { exec.awaitTermination(1,
	 * TimeUnit.DAYS); } catch (InterruptedException ex) { }
	 * 
	 * } catch (Exception e) { e.printStackTrace(); throw new
	 * org.apache.thrift.TException("backup didn't copy all"); } finally {
	 * backupCopyLock.unlock(); // this.writeLock.unlock(); }
	 * 
	 * }
	 */
}

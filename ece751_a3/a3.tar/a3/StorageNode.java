import java.io.*;
import java.net.InetSocketAddress;
import java.util.*;

import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.utils.*;

import org.apache.log4j.*;

public class StorageNode {
	static Logger log;
	volatile InetSocketAddress primaryAddress;
	String zkConnectString;
	String zkNode;
	CuratorFramework curClient;
	
	
	StorageNode(String zkConnectString, String zkNode) {
		this.zkConnectString = zkConnectString;
		this.zkNode = zkNode;
	}

	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
		log = Logger.getLogger(StorageNode.class.getName());

		if (args.length != 4) {
			System.err.println("Usage: java StorageNode host port zkconnectstring zknode");
			System.exit(-1);
		}
		
		StorageNode node = new StorageNode(args[2], args[3]);
		node.execute(args);

	}

	private void execute(String[] args) throws TTransportException, Exception {
		curClient = CuratorFrameworkFactory.builder().connectString(args[2])
				.retryPolicy(new RetryNTimes(10, 1000)).connectionTimeoutMs(1000).sessionTimeoutMs(1000).build();

		curClient.start();
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				curClient.close();
			}
		});
		

		KeyValueService.Processor<KeyValueService.Iface> processor = new KeyValueService.Processor<>(
				new KeyValueHandler(args[0], Integer.parseInt(args[1]), curClient, args[3]));
		TServerSocket socket = new TServerSocket(Integer.parseInt(args[1]));
		TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(socket);
		sargs.protocolFactory(new TBinaryProtocol.Factory());
		sargs.transportFactory(new TFramedTransport.Factory());
		sargs.processorFactory(new TProcessorFactory(processor));
		sargs.maxWorkerThreads(64);
		TServer server = new TThreadPoolServer(sargs);
		log.info("Launching server");

		new Thread(new Runnable() {
			public void run() {
				server.serve();
			}
		}).start();

		// TODO: create an ephemeral node in ZooKeeper
		// curClient.create(...)
		
		curClient.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(args[3] + "/child", (args[0] + ":" + args[1]).getBytes());
		
		InetSocketAddress primaryNode = getPrimary();
		log.info("length of child " + curClient.getChildren().forPath(zkNode).size());
		curClient.sync();
		if (Integer.parseInt(args[1]) != primaryNode.getPort() || !args[0].equals(primaryNode.getHostName())) {
			while (true) {
				TTransport transport = null;
				try {
					TSocket sock = new TSocket(args[0], Integer.parseInt(args[1]));
					transport = new TFramedTransport(sock, 1000000000);
					transport.open();
					TProtocol protocol = new TBinaryProtocol(transport);
					KeyValueService.Client client = new KeyValueService.Client(protocol);
					log.info("getting backup");
					client.fullCopyFromPrimaryToBackup();
					log.info("full copy done");
					break;
				} catch (Exception e) {
					log.info("retrying");
//					Thread.sleep(200);
				} finally {
					 transport.close();
				}
			}
		} else {
			TTransport transport = null;
			try {
				TSocket sock = new TSocket(args[0], Integer.parseInt(args[1]));
				transport = new TFramedTransport(sock);
				transport.open();
				TProtocol protocol = new TBinaryProtocol(transport);
				KeyValueService.Client client = new KeyValueService.Client(protocol);
				log.info("setting primary");
				client.setPrimary(true);
			} catch (Exception e) {
				
			} finally {
				 transport.close();
			}
		}
	}

	InetSocketAddress getPrimary() throws Exception {
		while (true) {
			curClient.sync();
			List<String> children = curClient.getChildren().forPath(zkNode);
			if (children.size() == 0) {
				log.error("No primary found");
				Thread.sleep(100);
				continue;
			}
			Collections.sort(children);
			byte[] data = curClient.getData().forPath(zkNode + "/" + children.get(0));
			String strData = new String(data);
			String[] primary = strData.split(":");
			log.info("Found primary " + strData);
			return new InetSocketAddress(primary[0], Integer.parseInt(primary[1]));
		}
	}

}

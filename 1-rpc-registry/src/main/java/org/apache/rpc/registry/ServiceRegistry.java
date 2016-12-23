package org.apache.rpc.registry;

import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 服务注册 ，ZK 在该架构中扮演了“服务注册表”的角色，用于注册所有服务器的地址与端口，并对客户端提供服务发现的功能
 * 
 */
public class ServiceRegistry {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(ServiceRegistry.class);

	private CountDownLatch latch = new CountDownLatch(1);

	private String registryAddress;

	public ServiceRegistry(String registryAddress) {
		//zookeeper的地址
		this.registryAddress = registryAddress;
	}

	/**
	 * 注册服务
	 * 
	 * @param data
	 */
	public void register(String data) {
		if (data != null) {
			// 连接zookeeper
			ZooKeeper zk = connectServer();
			if (zk != null) {
				// 创建数据节点
				createNode(zk, data);
			}
		}
	}

	/**
	 * 创建zookeeper链接，监听
	 * 
	 * @return
	 */
	private ZooKeeper connectServer() {
		ZooKeeper zk = null;
		try {
			zk = new ZooKeeper(registryAddress, Constant.ZK_SESSION_TIMEOUT,
					new Watcher() {
						public void process(WatchedEvent event) {
							if (event.getState() == Event.KeeperState.SyncConnected) {
								latch.countDown();
							}
						}
					});
			latch.await();
		} catch (Exception e) {
			LOGGER.error("", e);
		}
		return zk;
	}

	/**
	 * 创建节点
	 * 
	 * @param zk
	 * @param data
	 */
	private void createNode(ZooKeeper zk, String data) {
		try {
			byte[] bytes = data.getBytes();
			if (zk.exists(Constant.ZK_REGISTRY_PATH, null) == null) {
				zk.create(Constant.ZK_REGISTRY_PATH, null, Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT);
			}

			String path = zk.create(Constant.ZK_DATA_PATH, bytes,
					Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
			LOGGER.debug("create zookeeper node ({} => {})", path, data);
		} catch (Exception e) {
			LOGGER.error("", e);
		}
	}
}
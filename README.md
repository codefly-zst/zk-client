# zk-client
zookeeper客户端，基于zkclient实现的功能封装，对于zookeeper节点IP地址，支持从第三方获取。

# 使用指南

# 初始化zkconfig实例。
在未指定chrootPath情况下，默认情况下以groupName+appName作为命令空间，groupName通常是你所在部门或所在组的名称，appName通常代表你的应用程序名称。 

 对于权限认证采用用户名/密码方式，以groupName+appName为用户名。
 
 对于zookeeper节点Ip配置，支持从第三方获取，需在配置中指定第三方地址。

# 创建ZkClientManager实例，注入zkconfig。
通过该实例为所有功能的入口，建议通过其访问其它功能，除非特殊情况，否则不应该跳过它直接操作底层相关实例。

# 分布式ID生成器
提供基于版本号、顺序节点两种生成方式，针对同一idroot，将共用同一生成器，不同idroot将采用不同生成器。

因此，如果你的项目中在多个地方需要生成唯一ID,而且希望它们互不干扰，你可以通过设置idroot来使用不同的生成器。使用方式很简单

  zkClientManager.generatedId()
  
  zkClientManager.generateId(idroot)
  
  zkClientManager.generateIdByVersion(idroot);
  

# 配置中心
考虑到配置数据量有可能超过1M,以及配置Key较多的情况，为了避免单节点数据内容过多，以及节点数量过多的问题，客户端没有采用将所有数据写入到一个节点，也没有采用每个Key对应一个节点。

而是由应用方来决定节点数量，以及每个节点的内容。

客户端可以通过实现IZkConfigHandler来提供自己的注册信息，及信息变化后的回调处理

public interface IZkConfigHandler {
	
	Map<String,Properties> loadConfig();
	
	void handleDataChange(String key,Properties prop);
}


# Master-Slae模式
通过配置enableMaster,可以启用master选举，在启用master选举后，zkClientManager创建过程即开始竞争选举。

选举节点默认提交信息为自己IP地址，可以通过实现IMasterElectRegistry接口，并注入zkconfig来实现自定义的提交信息。

同样对于回调，也可能通过实现IMasterElectCallBack来处理业务中的回调后处理。

public interface IMasterElectCallBack {

	/**
	 * master节点变化时的回调处理器
	 * 
	 * @param isMaster       当前节点是否为Master节点
	 * @param masterJsonInfo master节点信息
	 * @param localJsonInfo  当前节点信息
	 * @param nodeMap        所有节点列表
	 * @return
	 */
	Object handleMasterDelete(boolean isMaster, String masterJsonInfo, String localJsonInfo, Map<String, String> nodeMap);

	/**
	 * 节点列表变化时的回调处理器
	 * 
	 * @param isMaster       当前节点是否为Master节点
	 * @param masterJsonInfo master节点信息
	 * @param localJsonInfo  当前节点信息
	 * @param nodeMap        所有节点列表
	 * @return
	 */
	Object handleChildChange(boolean isMaster, String masterJsonInfo, String localJsonInfo, Map<String, String> nodeMap);
}


# 分布式锁
对于分布式锁，支持类似公平锁与非公平锁两种方式，及阻塞与非阻塞两种方式。

对于非公平锁，所有应用通过竞争同一临时节点来实现；

对于公平锁，通过生成临时节点列表来实现，其中已经避免了羊群效应，以及应对节点宕机，监听失败，导致永久无法获取锁的情况。

同样，针对同一lockroot将竞争同一节点，所以如果在项目中的不同地方，需要不同的分布式锁，则可以通过不同的lockroot实现。


    zkClientManager.getSimpleDistributedLock()
    
    zkClientManager.getSimpleDistributedLock(String lockName)
    
    zkClientManager.getFairLock()
    
    zkClientManager.getFairLock(String lockName)
    

# 分布式队列
对于分布式队列，提供了阻塞和非阻塞两种读取方式，以及从头部、从尾部取值方式。

此外为了读高读性能，内部对于读操作维护了两个缓存队列进行读取，该队列通过监听节点变化进行变更，同时为了避免应用中多线程操作，采用了阻塞队列。

  此外，对于同一queueRoot的实例，为了避免创建相同IZkQueue的实例，采用了加锁同步控制，原因是不应该允许同时创建queuRoot相同的simpleQueueImpl对象，虽然他们只有一个有效，其余会在很短暂的时候失效，由JVM回收，但该对象其开销较大，涉及到网络IO、锁等等，所以new这个操作进行了控制防止。
  
  synchronized (lock) { 
			 zkQueue = zkQueueMap.get(queueRoot);
			 if(zkQueue == null) {
				 zkQueueMap.putIfAbsent(queueRoot, new SimpleQueueImpl(zkClient, queueRoot));
			 }
		}
  
# zkClient重启
当底层zkClient因未知异常而终止时，客户端可以进行重启操作，该方法将重新初始化底层zkClient 

zkClientManager.restart()。


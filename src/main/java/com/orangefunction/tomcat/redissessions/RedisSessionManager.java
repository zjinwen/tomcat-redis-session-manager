package com.orangefunction.tomcat.redissessions;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.catalina.Lifecycle;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.LifecycleListener;
import org.apache.catalina.LifecycleState;
import org.apache.catalina.Loader;
import org.apache.catalina.Session;
import org.apache.catalina.Valve;
import org.apache.catalina.session.ManagerBase;
import org.apache.catalina.util.LifecycleSupport;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;


import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;

public class RedisSessionManager extends ManagerBase implements Lifecycle {

	enum SessionPersistPolicy {
		DEFAULT, SAVE_ON_CHANGE, ALWAYS_SAVE_AFTER_REQUEST;

		static SessionPersistPolicy fromName(String name) {
			for (SessionPersistPolicy policy : SessionPersistPolicy.values()) {
				if (policy.name().equalsIgnoreCase(name)) {
					return policy;
				}
			}
			throw new IllegalArgumentException("Invalid session persist policy [" + name + "]. Must be one of "
					+ Arrays.asList(SessionPersistPolicy.values()) + ".");
		}
	}

	protected byte[] NULL_SESSION = "null".getBytes();

	private final Log log = LogFactory.getLog(RedisSessionManager.class);

	protected String host = "localhost";
	protected int port = 6379;
	protected int database = 0;
	protected int numPerNode = 20;
	protected String password = null;
	protected int timeout =60000;
	protected String sentinelMaster = null;
	Set<String> sentinelSet = null;
	Set<String> clusterSet = null;

	private  TreeMap<Long, ClusterNode> vnodes; 
	private List<ClusterNode> clusterNodes;
 

 
	protected RedisSessionHandlerValve handlerValve;
	protected ThreadLocal<RedisSession> currentSession = new ThreadLocal<>();
	protected ThreadLocal<SessionSerializationMetadata> currentSessionSerializationMetadata = new ThreadLocal<>();
	protected ThreadLocal<String> currentSessionId = new ThreadLocal<>();
	protected ThreadLocal<Boolean> currentSessionIsPersisted = new ThreadLocal<>();
	protected Serializer serializer;

	protected static String name = "RedisSessionManager";

	protected String serializationStrategyClass = "com.orangefunction.tomcat.redissessions.JavaSerializer";

	protected EnumSet<SessionPersistPolicy> sessionPersistPoliciesSet = EnumSet.of(SessionPersistPolicy.DEFAULT);

	/**
	 * The lifecycle event support for this component.
	 */
	protected LifecycleSupport lifecycle = new LifecycleSupport(this);

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public int getDatabase() {
		return database;
	}

	public void setDatabase(int database) {
		this.database = database;
	}

	public int getNumPerNode() {
		return numPerNode;
	}

	public void setNumPerNode(int numPerNode) {
		this.numPerNode = numPerNode;
	}

	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public void setSerializationStrategyClass(String strategy) {
		this.serializationStrategyClass = strategy;
	}

	public String getSessionPersistPolicies() {
		StringBuilder policies = new StringBuilder();
		for (Iterator<SessionPersistPolicy> iter = this.sessionPersistPoliciesSet.iterator(); iter.hasNext();) {
			SessionPersistPolicy policy = iter.next();
			policies.append(policy.name());
			if (iter.hasNext()) {
				policies.append(",");
			}
		}
		return policies.toString();
	}

	public void setSessionPersistPolicies(String sessionPersistPolicies) {
		String[] policyArray = sessionPersistPolicies.split(",");
		EnumSet<SessionPersistPolicy> policySet = EnumSet.of(SessionPersistPolicy.DEFAULT);
		for (String policyName : policyArray) {
			SessionPersistPolicy policy = SessionPersistPolicy.fromName(policyName);
			policySet.add(policy);
		}
		this.sessionPersistPoliciesSet = policySet;
	}

	public boolean getSaveOnChange() {
		return this.sessionPersistPoliciesSet.contains(SessionPersistPolicy.SAVE_ON_CHANGE);
	}

	public boolean getAlwaysSaveAfterRequest() {
		return this.sessionPersistPoliciesSet.contains(SessionPersistPolicy.ALWAYS_SAVE_AFTER_REQUEST);
	}

	public String getClusters() {
		StringBuilder clusters = new StringBuilder();
		for (Iterator<String> iter = this.clusterSet.iterator(); iter.hasNext();) {
			clusters.append(iter.next());
			if (iter.hasNext()) {
				clusters.append(",");
			}
		}
		return clusters.toString();
	}

	public void setClusters(String clusters) {
		if (null == clusters) {
			clusters = "";
		}
		String[] clusterArray = clusters.split(",");
		this.clusterSet = new HashSet<String>(Arrays.asList(clusterArray));
	}

	public String getSentinels() {
		StringBuilder sentinels = new StringBuilder();
		for (Iterator<String> iter = this.sentinelSet.iterator(); iter.hasNext();) {
			sentinels.append(iter.next());
			if (iter.hasNext()) {
				sentinels.append(",");
			}
		}
		return sentinels.toString();
	}

	public void setSentinels(String sentinels) {
		if (null == sentinels) {
			sentinels = "";
		}

		String[] sentinelArray = sentinels.split(",");
		this.sentinelSet = new HashSet<String>(Arrays.asList(sentinelArray));
	}

	public Set<String> getSentinelSet() {
		return this.sentinelSet;
	}

	public Set<String> getClusterSet() {
		return this.clusterSet;
	}

	public String getSentinelMaster() {
		return this.sentinelMaster;
	}

	public void setSentinelMaster(String master) {
		this.sentinelMaster = master;
	}

	@Override
	public int getRejectedSessions() {
		// Essentially do nothing.
		return 0;
	}

	public void setRejectedSessions(int i) {
		// Do nothing.
	}
	public RedisAdvancedClusterCommands<byte[], byte[]> getRedisCommand(String key) {
		if(key==null) {
		    ClusterNode node = vnodes.get(vnodes.firstKey());
			return node.getConnection();
		}
		SortedMap<Long, ClusterNode> tail = vnodes.tailMap(hash(key)); 
	    ClusterNode node =null;
        if (tail.size() == 0) {  
            node = vnodes.get(vnodes.firstKey()); 
        }else {
        	node = tail.get(tail.firstKey());
        }
        return node.getConnection();
	}

  
	@Override
	public void load() throws ClassNotFoundException, IOException {

	}

	@Override
	public void unload() throws IOException {

	}

	/**
	 * Add a lifecycle event listener to this component.
	 *
	 * @param listener
	 *            The listener to add
	 */
	@Override
	public void addLifecycleListener(LifecycleListener listener) {
		lifecycle.addLifecycleListener(listener);
	}

	/**
	 * Get the lifecycle listeners associated with this lifecycle. If this Lifecycle
	 * has no listeners registered, a zero-length array is returned.
	 */
	@Override
	public LifecycleListener[] findLifecycleListeners() {
		return lifecycle.findLifecycleListeners();
	}

	/**
	 * Remove a lifecycle event listener from this component.
	 *
	 * @param listener
	 *            The listener to remove
	 */
	@Override
	public void removeLifecycleListener(LifecycleListener listener) {
		lifecycle.removeLifecycleListener(listener);
	}

	/**
	 * Start this component and implement the requirements of
	 * {@link org.apache.catalina.util.LifecycleBase#startInternal()}.
	 *
	 * @exception LifecycleException
	 *                if this component detects a fatal error that prevents this
	 *                component from being used
	 */
	@Override
	protected synchronized void startInternal() throws LifecycleException {
		super.startInternal();

		setState(LifecycleState.STARTING);

		Boolean attachedToValve = false;
		for (Valve valve : getContainer().getPipeline().getValves()) {
			if (valve instanceof RedisSessionHandlerValve) {
				this.handlerValve = (RedisSessionHandlerValve) valve;
				this.handlerValve.setRedisSessionManager(this);
				log.info("Attached to RedisSessionHandlerValve");
				attachedToValve = true;
				break;
			}
		}

		if (!attachedToValve) {
			String error = "Unable to attach to session handling valve; sessions cannot be saved after the request without the valve starting properly.";
			log.fatal(error);
			throw new LifecycleException(error);
		}

		try {
			initializeSerializer();
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
			log.fatal("Unable to load serializer", e);
			throw new LifecycleException(e);
		}

		log.info("Will expire sessions after " + getMaxInactiveInterval() + " seconds");

		initializeDatabaseConnection();

		setDistributable(true);
	}

	/**
	 * Stop this component and implement the requirements of
	 * {@link org.apache.catalina.util.LifecycleBase#stopInternal()}.
	 *
	 * @exception LifecycleException
	 *                if this component detects a fatal error that prevents this
	 *                component from being used
	 */
	@Override
	protected synchronized void stopInternal() throws LifecycleException {
		if (log.isDebugEnabled()) {
			log.debug("Stopping");
		}

		setState(LifecycleState.STOPPING);
		
		if (clusterNodes != null) {
			for(ClusterNode n:clusterNodes) {
				n.close();
			}
		}
		super.stopInternal();
	}

	@Override
	public Session createSession(String requestedSessionId) {
	   return createByCluster(requestedSessionId);
		
	}

	private Session createByCluster(String requestedSessionId) {
		RedisSession session = null;
		String sessionId = null;
		String jvmRoute = getJvmRoute();
		try {
			if (null != requestedSessionId) {
				sessionId = sessionIdWithJvmRoute(requestedSessionId, jvmRoute);
				if (!getRedisCommand(sessionId).setnx(sessionId.getBytes(), NULL_SESSION) ) {
					sessionId = null;
				}
			} else {
				do {
					sessionId = sessionIdWithJvmRoute(generateSessionId(), jvmRoute);
				} while (!getRedisCommand(sessionId).setnx(sessionId.getBytes(), NULL_SESSION) ); // 1 = key set; 0 = key already
																						// existed
			}

			if (null != sessionId) {
				session = (RedisSession) createEmptySession();
				session.setNew(true);
				session.setValid(true);
				session.setCreationTime(System.currentTimeMillis());
				session.setMaxInactiveInterval(getMaxInactiveInterval());
				session.setId(sessionId);
				session.tellNew();
			}
			currentSession.set(session);
			currentSessionId.set(sessionId);
			currentSessionIsPersisted.set(false);
			currentSessionSerializationMetadata.set(new SessionSerializationMetadata());
			if (null != session) {
				try {
					saveInternal(  session, true);
				} catch (IOException ex) {
					log.error("Error saving newly created session: " + ex.getMessage());
					currentSession.set(null);
					currentSessionId.set(null);
					session = null;
				}
			}
		} finally {

		}

		return session;
	}

	
	private String sessionIdWithJvmRoute(String sessionId, String jvmRoute) {
		if (jvmRoute != null) {
			String jvmRoutePrefix = '.' + jvmRoute;
			return sessionId.endsWith(jvmRoutePrefix) ? sessionId : sessionId + jvmRoutePrefix;
		}
		return sessionId;
	}

	@Override
	public Session createEmptySession() {
		return new RedisSession(this);
	}

	@Override
	public void add(Session session) {
		try {
			save(session);
		} catch (IOException ex) {
			log.warn("Unable to add to session manager store: " + ex.getMessage());
			throw new RuntimeException("Unable to add to session manager store.", ex);
		}
	}

	@Override
	public Session findSession(String id) throws IOException {
		RedisSession session = null;

		if (null == id) {
			currentSessionIsPersisted.set(false);
			currentSession.set(null);
			currentSessionSerializationMetadata.set(null);
			currentSessionId.set(null);
		} else if (id.equals(currentSessionId.get())) {
			session = currentSession.get();
		} else {
			byte[] data = loadSessionDataFromRedis(id);
			if (data != null) {
				DeserializedSessionContainer container = sessionFromSerializedData(id, data);
				session = container.session;
				currentSession.set(session);
				currentSessionSerializationMetadata.set(container.metadata);
				currentSessionIsPersisted.set(true);
				currentSessionId.set(id);
			} else {
				currentSessionIsPersisted.set(false);
				currentSession.set(null);
				currentSessionSerializationMetadata.set(null);
				currentSessionId.set(null);
			}
		}

		return session;
	}

	@SuppressWarnings("deprecation")
	public void clear() {
		for(ClusterNode c:clusterNodes) {
			c.getConnection().flushdb();
		}
	}

	public int getSize() throws IOException {
		int size=0;
		for(ClusterNode c:clusterNodes) {
			size+=c.getConnection().dbsize();
		}
		 return size;
	}

	public String[] keys() throws IOException {
		return new String[] {};
	}

	public byte[] loadSessionDataFromRedis(String id) throws IOException {
		byte[] data = getRedisCommand(id).get(id.getBytes());
		if (data == null) {
			log.trace("Session " + id + " not found in Redis");
		}
		return data;
	}

	public DeserializedSessionContainer sessionFromSerializedData(String id, byte[] data) throws IOException {
		log.trace("Deserializing session " + id + " from Redis");

		if (Arrays.equals(NULL_SESSION, data)) {
			log.error("Encountered serialized session " + id + " with data equal to NULL_SESSION. This is a bug.");
			throw new IOException("Serialized session data was equal to NULL_SESSION");
		}

		RedisSession session = null;
		SessionSerializationMetadata metadata = new SessionSerializationMetadata();

		try {
			session = (RedisSession) createEmptySession();

			serializer.deserializeInto(data, session, metadata);

			session.setId(id);
			session.setNew(false);
			session.setMaxInactiveInterval(getMaxInactiveInterval());
			session.access();
			session.setValid(true);
			session.resetDirtyTracking();

			if (log.isTraceEnabled()) {
				log.trace("Session Contents [" + id + "]:");
				Enumeration<?> en = session.getAttributeNames();
				while (en.hasMoreElements()) {
					log.trace("  " + en.nextElement());
				}
			}
		} catch (ClassNotFoundException ex) {
			log.fatal("Unable to deserialize into session", ex);
			throw new IOException("Unable to deserialize into session", ex);
		}

		return new DeserializedSessionContainer(session, metadata);
	}

	public void save(Session session) throws IOException {
		save(session, false);
	}

	public void save(Session session, boolean forceSave) throws IOException {
		  saveInternal(session, forceSave);
	}

	protected boolean saveInternal(  Session session, boolean forceSave) throws IOException {
		Boolean error = true;
 
		try {
			log.trace("Saving session " + session + " into Redis");

			RedisSession redisSession = (RedisSession) session;

			if (log.isTraceEnabled()) {
				log.trace("Session Contents [" + redisSession.getId() + "]:");
				Enumeration<?> en = redisSession.getAttributeNames();
				while (en.hasMoreElements()) {
					log.trace("  " + en.nextElement());
				}
			}

			byte[] binaryId = redisSession.getId().getBytes();

			Boolean isCurrentSessionPersisted;
			SessionSerializationMetadata sessionSerializationMetadata = currentSessionSerializationMetadata.get();
			byte[] originalSessionAttributesHash = sessionSerializationMetadata.getSessionAttributesHash();
			byte[] sessionAttributesHash = null;
			if (forceSave || redisSession.isDirty()
					|| null == (isCurrentSessionPersisted = this.currentSessionIsPersisted.get())
					|| !isCurrentSessionPersisted || !Arrays.equals(originalSessionAttributesHash,
							(sessionAttributesHash = serializer.attributesHashFrom(redisSession)))) {

				log.trace("Save was determined to be necessary");

				if (null == sessionAttributesHash) {
					sessionAttributesHash = serializer.attributesHashFrom(redisSession);
				}

				SessionSerializationMetadata updatedSerializationMetadata = new SessionSerializationMetadata();
				updatedSerializationMetadata.setSessionAttributesHash(sessionAttributesHash);
				getRedisCommand(redisSession.getId()).set(binaryId, serializer.serializeFrom(redisSession, updatedSerializationMetadata));
				redisSession.resetDirtyTracking();
				currentSessionSerializationMetadata.set(updatedSerializationMetadata);
				currentSessionIsPersisted.set(true);
			} else {
				log.trace("Save was determined to be unnecessary");
			}

			log.trace(
					"Setting expire timeout on session [" + redisSession.getId() + "] to " + getMaxInactiveInterval());

			this.getRedisCommand(redisSession.getId()).expire(binaryId, getMaxInactiveInterval());
			
			error = false;
			return error;
		} catch (IOException e) {
			log.error(e.getMessage());
			throw e;
		} finally {

		}
	}

 

	@Override
	public void remove(Session session) {
		remove(session, false);
	}

	@Override
	public void remove(Session session, boolean update) {//not as pre
		 byte[] bid = session.getId().getBytes();
		 this.getRedisCommand(session.getId()).del(bid);
	}

	public void afterRequest() {
		RedisSession redisSession = currentSession.get();
		if (redisSession != null) {
			try {
				if (redisSession.isValid()) {
					log.trace("Request with session completed, saving session " + redisSession.getId());
					save(redisSession, getAlwaysSaveAfterRequest());
				} else {
					log.trace("HTTP Session has been invalidated, removing :" + redisSession.getId());
					remove(redisSession);
				}
			} catch (Exception e) {
				log.error("Error storing/removing session", e);
			} finally {
				currentSession.remove();
				currentSessionId.remove();
				currentSessionIsPersisted.remove();
				log.trace("Session removed from ThreadLocal :" + redisSession.getIdInternal());
			}
		}
	}

	@Override
	public void processExpires() {
		// We are going to use Redis's ability to expire keys for session expiration.

		// Do nothing.
	}

	private void initializeDatabaseConnection() throws LifecycleException {
		try {
			log.info("clusters is  " + clusterSet);
			Set<String> clusterSet = getClusterSet();
			int maxConnection=3;
			int maxStringConnection=0;
			if (clusterSet == null || clusterSet.size() == 0) {
				throw new IllegalArgumentException("pls set clusterSet ");
			}
			List<ClusterNode> clusterNodes=new ArrayList<ClusterNode>(clusterSet.size());
			for (String uri : clusterSet) {
				    ClientResources res = DefaultClientResources.builder().build();
					RedisURI redisUri = RedisURI.create(uri);
					RedisClusterClient clusterClient = RedisClusterClient.create(res,redisUri);
					List<StatefulRedisClusterConnection<byte[], byte[]>> 
					connections=new ArrayList<StatefulRedisClusterConnection<byte[], byte[]>>(maxConnection);
					List<StatefulRedisClusterConnection<String,String>>  stringConnections=new ArrayList<StatefulRedisClusterConnection<String,String>>(maxStringConnection);
					for(int i=0;i<maxConnection;i++) {
						StatefulRedisClusterConnection<byte[], byte[]> connection = clusterClient.connect(ByteArrayCodec.INSTANCE);
						connections.add(connection);
					}
					for(int i=0;i<maxStringConnection;i++) {
						StatefulRedisClusterConnection<String,String> connection = clusterClient.connect();
						stringConnections.add(connection);
					}
					ClusterNode cluster=new ClusterNode(uri,clusterClient,res,connections,stringConnections);
					clusterNodes.add(cluster);
			}
			setClusterNodes(clusterNodes);
		} catch (Exception e) {
			e.printStackTrace();
			throw new LifecycleException("Error connecting to Redis", e);
		}
	}
	
	 
	private  void setClusterNodes(List<ClusterNode> clusterNodes) {
		TreeMap<Long, ClusterNode> vnodes = new TreeMap<Long, ClusterNode>();
		int i=0;
		for(ClusterNode n:clusterNodes) {
			i=0;
			for(;i<numPerNode;i++) {
				vnodes.put(hash(n.name+i), n);
			}
		}
		this.vnodes=vnodes;
		this.clusterNodes=clusterNodes;
	}
	 private long hash(String key) {
	        MessageDigest md5 = null;
			if (md5 == null) {
	            try {
	                md5 = MessageDigest.getInstance("MD5");
	            } catch (NoSuchAlgorithmException e) {
	                throw new IllegalStateException("no md5 algorythm found");
	            }
	        }
	        md5.reset();
	        md5.update(key.getBytes());
	        byte[] bKey = md5.digest();
	        long res = ((long) (bKey[3] & 0xFF) << 24) | ((long) (bKey[2] & 0xFF) << 16) | ((long) (bKey[1] & 0xFF) << 8)
	                | (long) (bKey[0] & 0xFF);
	        return res & 0xffffffffL;
	    }
	private void initializeSerializer() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
		log.info("Attempting to use serializer :" + serializationStrategyClass);
		serializer = (Serializer) Class.forName(serializationStrategyClass).newInstance();

		Loader loader = null;

		if (getContainer() != null) {
			loader = getContainer().getLoader();
		}

		ClassLoader classLoader = null;

		if (loader != null) {
			classLoader = loader.getClassLoader();
		}
		serializer.setClassLoader(classLoader);
	}

	// Connection Pool Config Accessors

	// - from org.apache.commons.pool2.impl.GenericObjectPoolConfig

 
 

 
 
 

	 

	 

	 

 
 
}

class DeserializedSessionContainer {
	public final RedisSession session;
	public final SessionSerializationMetadata metadata;

	public DeserializedSessionContainer(RedisSession session, SessionSerializationMetadata metadata) {
		this.session = session;
		this.metadata = metadata;
	}
}

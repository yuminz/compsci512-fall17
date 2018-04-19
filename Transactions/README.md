## CPS 512 - Transactions Write Up

![API Diagram] (img/transactions.jpg)

### Intro

We have implemented serializable ACID transactions using two-phase locking (2PL) and two-phase commit (2PC). The diagram above shows the overall architecture of our program and also the API for each class.

At a high level you can see that there is a central LoadMaster actor who tells each of the TransactionApplication actors to perform some transactions via their KVClient. Each command from the LoadMaster is translated into one of the five KVClient API calls:

* Begin
* Read(key)
* Write(key, value)
* Commit
* Abort 

We designed it in such a way that, if there is no current transaction, the next call will always be `begin`.  If a transaction has begun, it will ensure at least one read or write is performed before trying to commit or abort. Once at least one read or write has been performed in the transaction, it is possible for the application to call commit() or abort().  We have set the probability of abort calls to be 3% of the total number of commands to ensure that not too many commands are aborted, so we can have at least some committed transactions. Another role of the LoadMaster/TransactionApplication is collecting the stats for our program. We have modified the Stats class to collect the following information:  

* The total messages from LoadMaster;
* Total number of transactions;
* Total number of transactions that succeed;
* Total number of transactions that abort;
* Total number of transactions that fail to acquire locks;
* The number of transactions aborted early (due to an expired session lease);
* The number of transactions aborted during commit. 

### Implementation

The KVClient’s API is the same as described in the lab description. Our main design point for KVClient is the creation of the KVClientHelper actor as the lease manager and transaction manager. This actor acquires and renews all of the session leases for the current transaction with the corresponding stores. This ensures that when KVClient blocks on reads and writes, it will not lose the leases which the KVClient currently holds. The KVClientHelper is logically just another thread running on the same machine as the TransactionApplication/KVClient actor. Without the implementation of the helper actor, the KVClient could be blocked on a read or write when the lease for some other lock which the client is currently holding might be about to expire, but the client cannot send out the RenewLease message to the KVStore since it is blocked. By implementing a helper actor, we allow the KVClient to be blocked on an acquire while the helper will handle the lease renewals. In begin(), we create a new KVClientHelper, passing it a unique transaction id. Every time when read, write, or commit is called in KVClient, it sends the corresponding message to the helper and waits for the helper to respond (while waiting, the client is blocked). For abort, the client just sends the message to the helper but does not block. If the read or write is successful, the KVClient stores the (key, value) pair in its cache.  Otherwise, it throws the corresponding exception to the application. In later reads and writes, if the lock for the requested key has already been acquired, the value can be returned from the cache instead of interacting with the helper. If a commit is successful, the commit() call will return without error and will clear its cache. Otherwise, an exception will be thrown.

As shown the diagram above, the helpers do the most of the work. They talk to both the KVClient and the KVStores. When the client tells the helper it wants to read or write a given key, the helper will send a message to the KVStore that stores the given key to acquire the lock. It sets a timer to wait for `acquireTimeout`, which is a parameter which the user can define. It will return either the value from KVStore to the client on success or a failure message if it can’t acquire the lock within acquireTimeout. 

For an in progress transaction for which the app hasn’t yet called commit, the KVClientHelper maintains a session lease with each of the stores for which it holds a lock. Like Chubby, our helper only holds one session lease with each KVStore, even if the transaction involves multiple locks from the same store. The helper, upon being granted a lease, will schedule a timer to go off when half of the lease term has passed. When this timer goes off, the helper will send a RenewLease message to the corresponding KVStore. The helper will schedule another timer for a quarter of the lease time awaiting the renewal response from the KVStore. If it receives a renewal response, it schedules another timer for the new lease. If it doesn’t hear back in time, it assumes it has lost the lease and aborts the transaction early, notifying the KVClient. If this session lease expires and the client wasn’t able to renew the lease, because of a network partition, the helper will abort the transaction by releasing all other locks and will notify the KVClient. If concurrently the KVClient is blocking to acquire a different lock, an exception will be thrown from the KVClient to the application. Additionally, the KVClientHelper will send a RemoveFromWaitQueue message to the store with the new lock telling it to remove the aborted transaction from the waiting queue for this lock so it doesn’t later grant the lock to a transaction that no longer exists.

At any point, the application can choose to call abort(), which will abort the transaction early. On the client side, this will cause the KVClientHelper to cancel all of its session lease timers and to send AbortEarly messages to each of the relevant KVStores. On the KVStore side, upon receiving an AbortEarly message, it will free that transaction’s locks and will grant them to the next transaction on each of their waiting queues if there is one.

When the KVClient sends the commit message to the helper, the helper will first send a prepare message to all the participants, which are the KVStores participating in this transaction, and from then on the helper acts as the transaction manager. Each KVStore will check if the transaction id is indeed the lock holder for those locks, and if so, it will vote commit, otherwise it will vote abort. If it votes commit, the KVStore will be blocked indefinitely until it hears the overall transaction commit result back from the transaction manager. The KVStore won’t release the locks for this transaction until it hears the final result, even if there is a network partition. If the KVStore votes abort, it will immediately release all the locks the current transaction holds.

The KVClientHelper receives and tallies all the votes. If all the participants have voted commit, the transaction manager will send each KVStore the subset of key value pairs from the client’s cache that route to that store. If a unanimous commit vote isn’t obtained, KVClientHelper aborts the transaction and notifies the KVClient and all participating KVStores. If there is a network partition, the KVClientHelper may not receive all the votes before the VotingTimeout expires. At this point it will go ahead and directly abort the transaction based on the 2PC protocol. Before the helper actor can shut down, it must receive an acknowledgement from each participant regarding the Commit/AbortTransaction decision. The KVClientHelper will periodically resend the same decision message to the stores who haven’t yet acknowledged it. 

The KVStore side’s implementation is mostly covered in the paragraph above discussing the communication between the helper and the KVStore. The KVStore stores a map from transaction ID to the locks it currently holds. It also maintains information for each lock: current holder and waitingQueue. The KVStore grants the lock based on a FIFO order. On the KVStore side, when a given transaction’s session lease expires, its locks are released and granted to the next transactions on the waiting queues if available.

When a KVStore receives a Prepare message for a transaction, it is told which keys are involved in the transaction which this store is responsible for. It verifies that this transaction currently holds all of the locks for the keys that are involved in the transaction before voting to commit, otherwise it will vote to abort. Upon receiving a CommitTransaction message, it will receive the dirty data from the transaction and it will write the updates into its store. The KVStore will then send an acknowledgement back to the KVClientHelper. 

The PartitionSimulator will generate a random list of clients which are partitioned from the KVStores. This list is recomputed at a regular rate called partitionFrequency. When the PartitionSimulator determines the current set of partitioned clients, it sends this information to each of the KVStores. When a KVStore receives the UpdatePartition message from the PartitionSimulator, it will update its partition list and ignore all the messages from those partitioned clients. 

##### Scalability

In our design, each KVClientHelper functions as its own coordinator for the transaction. This is instead of having a randomly elected KVStore be chosen as the coordinator for all transactions, in which case that KVStore would become a bottleneck as the number of transactions increased.

During commit, the participants for a given transaction are only those who are in charge of at least one key involved in the transaction. This prevents waiting for votes from KVStores whose votes don’t actually matter in the committing of a transaction because their keys wouldn’t actually be affected. This becomes more important as you increase the number of stores.

When a KVStore votes to abort on a transaction, it immediately releases that transaction’s locks instead of waiting for the official decision from the KVClientHelper, because it knows the coordinator will not be able to obtain a unanimous vote to commit. This allows the KVStore to reduce the waiting time for other transactions to acquire these locks, potentially decreasing the number of transactions aborting due to acquire lock timeouts.

### Testing

To verify the correctness of our Transactions service, we have written 6 tests.

##### Read my writes
This test has five concurrent clients trying to write a value to the same key and then read that value. It verifies that each transaction reads the value it had previously written showing isolation of concurrent transactions because only one of them can hold the lock on the key at a given time.

##### Read updates from previously committed transactions
In this test there are two clients and the first commits a transaction updating two keys. The second client then begins a transaction reading the same two keys. It asserts that the values read are the same as what was written by the previously committed transaction. This test demonstrates the atomicity of multiple updates in a transaction and the isolation of concurrent transactions.

##### Don’t read updates from previously aborted transactions
In this test there are two clients and the first updates two keys but then aborts. The second client then begins a transaction reading the same two keys. It asserts that the values read are not what the aborted transaction updated but rather the previous values before the aborted transaction started. This test demonstrates the atomicity of multiple updates in a transaction and the isolation of concurrent transactions.

##### Transactions will not stay deadlocked forever
In this test we have two MockApplications, app0 and app1. We have app0 acquire the lock for key0 and app1 acquire the lock for key1. We then create a circular waiting for resources by having app0 try to acquire the lock for key1 and app1 try acquire the lock key0. We then wait for acquireTimeout to expire and show that neither of the apps is still deadlocked, meaning that each app has either acquired the lock or has aborted its transaction, but neither is still blocking.

##### Partition before commit
In this test we have one client who makes updates to a couple of keys, stored on two different KVStores. Before it commits, however, it becomes partitioned from one of the KVStores. Then, when it sends the Prepare messages to the KVStores, the partitioned KVStore never responds and the VotingTimeout on the KVClientHelper must expire. We verify that the transaction is then aborted when a TransactionAbortedException is thrown.

##### Partition after prepares sent
In this test we have two clients, one who starts a transaction and makes a couple of updates. Then when it commits, it first sends prepare messages to the KVStore. The KVStore then responds with a VoteCommit and immediately afterward, a network partition begins between the client and the store. This prevents the client from sending to the store CommitTransaction messages, so the store should maintain all of the locks the first client held until it receives CommitTransaction. We test this by having the second client try to acquire one of these locks and verify that the acquire times out. The implementation for this test is particularly interesting because in order to start the partition exactly after the KVStore sends VoteCommit to the KVClientHelper, we had to basically perform a man in the middle kind of attack on the KVClientHelper and the KVStore. We encourage you to look at how we did this in TransactionsTest.scala.


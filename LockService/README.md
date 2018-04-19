# Lock Service

### Overview


![] (img/image0.jpg)

We have implemented a lock service which allows multiple client applications running on separate machines to obtain locks from a central lock server. An application using the lock service will include the LockClient class and use its API to interact with the lock service. Our lock client is multithreaded, with each thread managing a single lock. We split the client into multiple actors to allow the main thread to block without affecting the lock client’s ability to renew leases. If the client ran on a single actor and it ever blocked to acquire a new lock, it is possible that the client wouldn’t be able to renew the lease for another of the currently held locks. Having an actor manage a single lock allows for that actor to block while awaiting a response and not affect the other locks. Similarly, the lock service is multithreaded, with each thread managing a single lock. By splitting the management of locks across multiple threads, we reduce the message traffic the main lock service actor has to handle, to prevent it from becoming a bottleneck.

 At a high level you can see that there is a central LoadMaster actor who tells each of the application actors to perform some locking operation. Each application interprets commands from the master and translates them into a lock acquire or release. To acquire/release a lock, the application uses the LockClient class. When a given lock is held by an application, the lock’s lease is managed (acquired/renewed) by a LockClientHelper. Each LockClientHelper manages the lease of a single lock; it is its own actor running on a separate thread from the application. The use of multiple actors for the lock client allows the application actor to block while not affecting the lock client’s ability to renew the leases. While they are separate actors, you can think of the application actor and all of its LockClientHelper actors as running on a single, multithreaded node. Our lock server is multithreaded as well, with a central LockService actor which interacts with all clients at first, but delegates the control of individual locks to LockManagers. Each LockManager is its own actor as well and interacts with all of the LockClientHelpers for its lock. We also created a PartitionSimulator which at a given rate will create a network partition between the lock server and a random set of clients.

To acquire a lock, the application will simply call client.acquire(name), where name is the name of the lock they want to acquire. LockClient must first contact the main LockService actor to be routed to the desired lock’s managing actor. Each lock controlled by the LockService is managed by its own LockManager actor. Once the LockClient knows the corresponding LockManager, it creates a new actor, called a LockClientHelper, to manage the acquisition and renewal of the lease on its own thread. The LockClientHelper communicates directly with the corresponding LockManager to acquire and renew its lease. The helper will first send an acquire message to the lock manager, who will grant the lock if currently no other application holds the lease. However, if the lock is currently held, the client is added to a waiting queue. The helper will block until it acquires the lease, or until LockWaitTime expires, in which case it returns an error to the application. 

Once a lock is held by an application, the renewal of the lease is handled by the corresponding LockClientHelper. The LockClientHelper, upon being granted a lease, will schedule a timer to go off when half of the lease term has passed. When this timer goes off, the LockClientHelper will send a RenewRequest message to the corresponding LockManager. The helper will block for another quarter of the lease time awaiting the renewal response from the manager. If it receives a renewal response, it schedules another timer for the new lease. If it doesn’t hear back in time, it assumes it has lost the lease and sends a LockLost message to the application if the lock is actively held by the application.

The lock client is able to “cache” a lock even when the application isn’t currently using it. When an application releases a lock, the LockClientHelper doesn’t immediately notify the lock manager. Instead, it will keep trying to renew the lease. However, in its RenewRequest message, it tells the manager that the application isn’t actively holding the lock, so it is okay for the manager to deny the lease renewal if there is another application on the waiting queue. If there is another application waiting for the lock, the manager will reply to the lock client helper with a denied renewal message, saying it no longer holds the lock. The LockManager will then grant the lease to the next application waiting.

If a lock is currently cached by the client and the application requests it, the client can respond with a lock granted message immediately instead of having to communicate with the lock manager. 

In the event of a network partition, a client will not be able to send a RenewalRequest to the lock manager. When the LockClientHelper sends a RenewalRequest to the lock manager, if it doesn’t hear a response within a quarter of the current lease time, it assumes it has lost the lock. If the application doesn’t currently hold the lock, nothing else happens. However, if the application is actively holding the lock, the client helper sends a LockLost message to the application. 

On the server side, if the LockManager doesn’t get a RenewalRequest from the current lock holder by the end of the current lease time, it says the current holder no longer holds the lock. It will then mark the lock as unheld, or grant it to the next application on the waiting queue if there is one.

When a lock manager receives an Acquire message from a client, if another client currently holds the lock, the new client is added to a waiting queue. The manager then sends a RecallRequest message to the current holder, telling it to go ahead and release the lock if possible. The client will either respond with an Accepted message if lock is simply cached and the application isn’t actively using it, or with a Denied message if the application is actively holding the lock. The lock manager will continue to periodically send these RecallRequests as long as there is someone on the waiting queue, in order to minimize the lock acquire latency.

Suppose we didn’t have the recall request. When a client releases a lock, the lock manager doesn’t find out about this until the next time the client renews the lease. However, this could be a considerable amount of time if the lease is long. This means that other clients waiting to acquire the lock might time out before they can acquire it. 
Having a recall message allows the manager to reclaim the unused lock from the client and grant it to another client quicker. Similar behavior can be achieved if you keep the lease times low. The benefit of using the recall message instead is that it allows you to keep a longer lease time, and recall messages are only sent if there is another client waiting. So using recall messages instead means less message traffic when there are no other clients waiting for the lock. 

To show the performance gains with using a recall message, we run the same test, once without the recall messages and once with. We set numNodes = 10, burstSize = 1, opsPerNode = 2, leaseTime = 5 seconds, lockWaitTime = 60, numLocks = 1, and turn off partition simulation by setting partition frequency to 70 seconds. Basically, every actor will first try to acquire and then release the same lock, so this test demonstrates how our service performs with a highly contended lock. When running this test without the recall messages, the runtime is 49 seconds When running the test with the recall messages, all 10 applications are able to acquire and release the lock in just 12 seconds.

Another interesting test to demonstrate the effects of network partitions uses the following parameters:
timeout = 90 seconds, numNodes = 5, burstSize = 5, opsPerNode = 20, leaseTime = 5 seconds, lockWaitTime = 5 seconds, partitionFrequency = 20 seconds, numLocks = 30
You will see that because of the network partitions, some locks are reported as lost to the applications. Additionally, you can see there are errors in acquiring locks. Another thing to observe is the percent of lock acquires that are satisfied by an already cached lock.


### Timing Scenarios
##### Acquire and Release

T: lease time 	
L: lock wait time

![] (img/image1.jpg)

The diagram above illustrates the scenario of two clients wanting to acquire the same lock. Client 1 first sends Acquire to the Lock Manager and gets a granted message with the lease term as ‘T’ back. Client 1 then schedules a timer of T/2 to send a renew message to the lock manager to renew its lease. The renew message includes if the lock is actively being used by the application. Client 1 will be blocked for T/4 time to wait for the renew lease request to be accepted by the Lock Manager. If accepted, the client will schedule another renew time to be half of the lease term. C1 only assumes it has the lock for T * (¾) to account for message delays in the network and to guarantee that only one client thinks it has the lock at a given time.

##### Recall

![] (img/image2.jpg)

The diagram above illustrates the recall procedure in our lock service. At the start, Client 1 is the lock holder but is not actively using the lock. Then Client 2 sends an acquire request to the lock manager. As soon as the manager receives the acquire request, it sends the recall request to the current lock holder, which is Client 1. Since Client 1 is not actively using the lock, it will accept the recall request and release the lock. When the lock manager receives the recall accepted message, it grants the lock to client 2. 


##### Network Partition

![] (img/image3.jpg)

The diagram above illustrates the scenario of a network partition. Client 1 is the lock holder originally, but a network partition starts and Client 1 cannot communicate with the lock manager. Therefore, as demonstrated in the diagram, the renew message is not able to reach the lock manager. When the lock manager’s lease timer goes off, it knows the current lock holder’s lease is no longer valid, so it grants the lock to Client 2 who is waiting for the lock (assuming Client 2 is first in the waiting queue). 



### Testing
We wrote 4 tests to verify the correctness of our lock service.
##### Mutual Exclusion
Our first test makes sure that if multiple actors request the same lock, a maximum of one of them will hold the lock at any given time. To test this we have five test actors all try to acquire the same lock and check that only one acquired it while the others timed out. We then have the holder release the lock and the others try to acquire it again. We verify that a different actor now holds the lock exclusively.

##### Network partition handling on the client side
In this test we verify that if the lock holder doesn’t hear a lease renewal message from the lock server by the end of the current lease, it knows that it no longer holds the lock and that the application receives the LockLost message. We have a test actor acquire the lock and then we create a network partition between the test actor and the lock server. We then wait a lease term and verify that the application no longer thinks it holds the lock.

##### Network partition handling on the server side
In this test we verify that if the lock holder is partitioned from the lock server and can’t renew its lease, the lock server is able to grant the lock to another client who requests it after the lease expires. To test this we first have a test actor acquire the lock and then create a partition between that client and the lock server. We then have another client request the lock and make sure that after a lease has gone by, the new client holds the lock.

##### Lock is cached by client if no one else requests it
In this test we verify that when the application releases the lock, if no other application requests the lock, the client keeps the lease renewed and “caching” the lock. We first have a test actor acquire the lock and then release it. We then verify that after a couple of lease terms, the lock manager still has the original client to be holding the lock but not actively using it.



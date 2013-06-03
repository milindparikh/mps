mps
===

Massive Publish Subscribe based on Kafka


mps is built as an publish/subscribe mechanism on top of Kafka in Erlang. This version of mps uses 
Apache Kafka 0.8. Apache Kafka's wire protocol enables it to communicate with other clients. The 
wire protocol, for such systems, is usually too low level to enable easy-to-use semantics at the 
the higher level. 

mps talks natively to Kafka; but hides the complexity of dealing with the wire protocol. We chose to 
expose two main interfaces: publish and subscribe. The archtypical function signature is simple.

publish/3 --> publish(Topic, Key, Value)
subscribe/4 --> subscribe (Topic, Key, Instance, CallbackFunction)

We provide additional useful variations of both publish and subscribe. 

We provide an sample implementation of publish/subscribe mechanism that lives at the user interface 
level to demonstrate the useful of such semantics. 

Three other features probably deserve mention: 
 
(a) STREAM and REPLAY 
      
We chose to expose two main modes of operations: STREAM and REPLAY

Streaming is the process through which a client "tags" on the stream in context of data flowing 
through in Kafka. This is the most efficient usage from a Kafka perspective. 

Replaying is when a client needs to look at the history of that Topic/Key. There is no current merge 
capabilities when the replay becomes in-sync with the stream. 


(b) KEY FILTERING 
       subscribe("Promotions", "music.*", <InstanceId>, <CB>) and 
       subscribe("Promotions", "music.rock.dvds", <InstanceId>, <CB>) 
       
       enable different events to flow into your subscription.


(c) SCALE

mps is riak_core enabled. This means that adding additional nodes to handle demand and deleting nodes
to shed load beccomes easy to do. 


BUILDING 

Currently riak_core depends on webmachine which depends on mochiweb. We are using R16B version of Erlang. 
THe version of mochiweb that deals with webmachine is not compatible with R16B. One must update to a later
version of mochiweb. 

Also for the sample example of the demo, we use cowboy as the webserver ; primarily for the websocket level. 
cowboy does not play nicely with the riak_core template for content serving (or at least it demonstrates 
our ignorance in working with cowboy). THere are some steps that must be taken after the rel is built on a 
manual basis.








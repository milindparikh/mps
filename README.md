mps
===

Massive Publish Subscribe based on Kafka

THIS IS CURRENTLY ALPHA SOFTWARE (WITH ALL OF THE ATTENDANT ISSUES). 

mps is built for eventing framework mechanism on top of Apache Kafka in Erlang. This version of mps 
uses Apache Kafka 0.8. Apache Kafka's wire protocol enables it to communicate with other clients. The 
wire protocol, for such systems, is usually too low level to enable easy-to-use semantics at the 
the higher level. 

mps talks natively to Kafka; but hides the complexity of dealing with the wire protocol. We chose to 
expose two main interfaces: publish and subscribe. The archtypical function signature is simple.

publish/3 --> publish(Topic, Key, Value)
subscribe/4 --> subscribe (Topic, Key, Instance, CallbackFunction)

We provide additional useful variations of both publish and subscribe. 

We provide an sample implementation of publish/subscribe mechanism that lives at the user interface 
level to demonstrate the usefulness of such semantics. 

Four other features probably deserve mention: 


1.  KEY FILTERING 

       add_subscription (Subscriber, {"Promotions", "com.*", <InstanceId>, <CB>})
       subscribe("Promotions", "com.*", <InstanceId>, <CB>)
           monitors chatter on the "com.*" key range 

       add_subscription (Subscriber, {"Promotions", "com.(amazon|ebay).sales.*", <InstanceId>, <CB>})
       subscribe("Promotions", "com.(amazon|ebay).sales.*", <InstanceId>, <CB>)
           monitors specific events that occur on 
                           com.amazon.sales.* 
                           com.ebay.sales.* 
                                            key range

       add_subscription (Subscriber, {"Promotions", "com.amazon.sales.orders", <InstanceId>, <CB>})
       subscribe("Promotions", "com.amazon.sales.orders", <InstanceId>, <CB>) 
            monitors even more specific events on 
                            com.amazon.sales.orders


2. MESSAGE BROADCASTING 
       publish ("Promotions", "com.(amazon|ebay|yahoo).sales.taximplications", Value) 
                publishes the Value on the key-tree implied by the key-expression AKA 

                  ["com",
                           "com.amazon","com.ebay","com.yahoo",
                                 "com.amazon.sales","com.ebay.sales","com.yahoo.sales",
                                           "com.amazon.sales.taximplications",
                                           "com.ebay.sales.taximplications",
                                           "com.yahoo.sales.taximplications"]



                       

3. STREAM and REPLAY 
      
We chose to expose two main modes of operations: STREAM and REPLAY in subscription


Streaming is the process through which a client "tags" on the stream in context of data flowing 
through in Kafka. This is the most efficient usage from a Kafka perspective. Streaming is simple.

	stream

       add_subscription (Subscriber, {"Promotions", "com.amazon.sales.orders", <InstanceId>, <CB>}) 

devolves to 

       add_subscription (Subscriber, {"Promotions", "com.amazon.sales.orders", <InstanceId>, <CB>}, stream)
			
Replaying is when a client needs to look at the history of that Topic/Key. There is no current merge 
capabilities when the replay becomes in-sync with the stream. The following verbs will be  supported in mps

	     {replay, from_begining, to_end}       %% the end as it exists at the start of replay
	     {replay, from_begining, to_infinity}  %% after playing catchup, behaves like stream on own channel
     
	     {replay, from_begining, {to_offset, M}}
	     {replay, {from_offset N}, {to_offset, M}}

Replaying is not free and in some cases, becomes expensive and in rare cases very expensive. The reason is 
essentially the client is creating his/her own channel without amortizing the cost with several other clients. 
But essentially, thanks to riak core (below), it really becomes an issue for expense rather than a question of 
doability. 




4. SCALE

mps is riak_core enabled. This means that adding additional nodes to handle demand and deleting nodes
to shed load should beccome easy to do. 


BUILDING 

Currently riak_core depends on webmachine which depends on mochiweb. We are using R16B version of Erlang. 
THe version of mochiweb that deals with webmachine is not compatible with R16B. One must update to a later
version of mochiweb. 

You must have a sane make and rebar. 

Steps : 
    1. git clone https://github.com/milindparikh/mps
    2. make rel 
          will give you some warnings and errors 
          To remove the errors, go to rebar.config of webmachine (under deps/webmachine)
             2.a change mochiweb tag from "1.5.1p3" to ""1.5.1p5"
             2.b remove the mochiweb directory under deps
	     2.c rerun make rel
                        It will now get the correct version of the mochiweb
             2.d ./rel/mps/bin/mps console will start the entire node
                  BUT YOU ARE NOT DONE YET... SEE BELOW

Also for the sample example of the demo, we use cowboy as the webserver ; primarily for the websocket level. 
cowboy does not play nicely with the riak_core template for content serving (or at least it demonstrates 
our ignorance in working with cowboy). THere are some steps that must be taken after the rel is built on a 
manual basis.

		STEP 2.e IS NOW DONE AS A PART OF THE make rel PROCESS

            2.e Copy the priv directory under apps/mps/src to two places 
                       rel/mps 
		       rel/mps/lib/mps-1        -> you will need to mkdir mps-1 under lib first


            2.f Under console, you are now in erlang world 
	    	  do a mps_pubsub:create_topics().
		  

            2.g 
                localhost:8080              gets you the Topic Subscription Client
		localhost:8080/publish      gets you the Topic Publishing Client 
           




 
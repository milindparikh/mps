-module(test).
-export([test/0, test1/1, test2/2]).

test() ->
    io:format("creating topics..."),
    mps:create_topics(),
    io:format("..done"),
    
    io:format("subscribing to keys.."),
    S = mps:new_subscriber(),
    S1 = mps:add_subscription(S, {"Topic1", "Key1", some, fun mps_event_handler_simple:mps_subscription_fun/4}),
    io:format("..done"),
    io:format("~p~n", [S1]),
    
    io:format("publisiing events..."),
    mps:publish("Topic1", "Key1", "Value1"),
    io:format("...done").

    
test1(NumParts) ->
    
    test1([], 0, NumParts, NumParts).

test1(ListPartitions, _Count, 0, _NumberOfPartitionsInRing) ->
    ListPartitions;

test1(ListPartitions, Count, RemainingPartitions, NumberOfPartitionsInRing)  -> 
    StartPartition = trunc(math:pow(2,160)/NumberOfPartitionsInRing)*Count,
    EndPartition = trunc(math:pow(2,160)/NumberOfPartitionsInRing)*(Count+1) -1,
    test1([{Count, StartPartition, EndPartition} | ListPartitions], Count+1, RemainingPartitions - 1,NumberOfPartitionsInRing).



test2(NumberOfPartitionsInRing, Count) ->
    StartPartition = trunc(math:pow(2,160)/NumberOfPartitionsInRing)*Count,
    EndPartition = trunc(math:pow(2,160)/NumberOfPartitionsInRing)*(Count+1) -1    ,
    topics_in_partition (65535, [], StartPartition, EndPartition).

topics_in_partition (-1, ListTopics, _StartPartition, _EndPartition)->
    ListTopics;

topics_in_partition (I, ListTopics, StartPartition, EndPartition)->
    B = <<I:16>>,
    C = mps_utils:hexstring(B),
    Index = riak_core_util:chash_key({term_to_binary(C), term_to_binary(C) }),
    <<IndexAsInt:160/integer>> = Index,
    case ((IndexAsInt >= StartPartition) and (IndexAsInt =< EndPartition)) of 
	true ->
	    topics_in_partition(I-1, [{C}| ListTopics], StartPartition, EndPartition);
	false ->
	    topics_in_partition(I-1,  ListTopics, StartPartition, EndPartition)
    end.

    
   
    
		     



    


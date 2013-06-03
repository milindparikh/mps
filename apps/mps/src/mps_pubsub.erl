-module(mps_pubsub). 
-export([subscribe/4, publish/3, create_topics/0]).

-define(REDUDANCY_FOR_SUBSCRIPTION, 1).
-define (TOPICRANGE, 2).
-define(NUMBEROFTOPICS,65535).

%% The main function that figures out the function to call on
%% the CallbackFun should be CallbackFun/4 with Instance, Topic, Key, Msg


subscribe(Topic, Key, I, CallbackFun) ->

    HTopic = hextopic(Topic),    %% THIS NEEDS TO BE A LITTLE MORE INTELLIGENT SUCH AS 
                                 %% ADDING REDUNDANCY AND KEY AS A PART OF THE FUNCTION CALL
                                 %% ADDITIONALLY THE add_handler NEEDS TO CHANGED TO HANDLE 
                                 %% RIAK_CORE STYLE VNODE DISTRIBUTION
    gen_event:add_handler(HTopic, mps_eventhandler_subscription, [CallbackFun, I, Topic, Key]).  



publish(Topic, _Key, Value) -> 
    HTopic = hextopic(Topic),
    gen_event:notify(HTopic, Value).

hextopic(Topic) ->    
    S = crypto:sha(Topic),
    B = binary_to_list(S),
    {L1, _L2} = lists:split(?TOPICRANGE, B),
    
    list_to_atom("Topic-"++hexstring(list_to_binary(L1))).

    
hexstring(Binary) when is_binary(Binary) ->
    lists:flatten(lists:map(
        fun(X) -> io_lib:format("~2.16.0b", [X]) end, 
        binary_to_list(Binary))).

    
create_topics() ->
    create_topics(?NUMBEROFTOPICS).

create_topics(0) ->
    ok;
create_topics(I) -> 
    B = <<I:16>>,
    HTopic = hextopic(B),
    
    gen_event:start({local, HTopic}),
    create_topics(I-1).


    

    

    

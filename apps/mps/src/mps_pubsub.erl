-module(mps_pubsub). 
-export([subscribe/4, publish/3, create_topics/0]).
-export ([get_hex_topics/2]).
-export([hextopic/1]).
-export([hexstring/1]).

-define(REDUDANCY_FOR_SUBSCRIPTION, 0).
-define (TOPICRANGE, 2).
-define(NUMBEROFTOPICS,65535).

%% The main function that figures out the function to call on
%% the CallbackFun should be CallbackFun/4 with Instance, Topic, Key, Msg


subscribe(Topic, KExpr, I, CallbackFun) ->
    HTopics = get_hex_topics(Topic, mps_utils:generate_keys_for_subscribe(KExpr)),
    

    %% THE FOLLOWING IS A NAIVE IMPLEMENTATION OF add_handler... USE SPAWN OR SOMETHING
    lists:foldl(fun (X, _A) ->
			gen_event:add_handler(X, mps_eventhandler_subscription, [CallbackFun, I, Topic, KExpr])
		end,
		[],
		HTopics),
    ok.





publish(Topic, KExpr, Value) -> 
    HTopics = get_hex_topics(Topic, mps_utils:generate_keys_for_publish(KExpr)),
    %% THE FOLLOWING IS A NAIVE IMPLEMENTATION OF NOTIFY... USE SPAWN OR SOMETHING
    lists:foldl(fun (X, _A) ->
			gen_event:notify(X, Value)
		end,
		[],
		HTopics),
    ok.





get_hex_topics (Topic, Keys) ->
    [hextopic(Topic++"-"++integer_to_list(N), K) || N <-lists:seq(0,?REDUDANCY_FOR_SUBSCRIPTION), K <- Keys].
    
hextopic(Topic, Key) ->    
    S = crypto:sha(Topic++Key),
    B = binary_to_list(S),
    {L1, _L2} = lists:split(?TOPICRANGE, B),
    
    list_to_atom("Topic-"++hexstring(list_to_binary(L1))).


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

    HTopic =  list_to_atom("Topic-"++hexstring(B)),
    
    gen_event:start({local, HTopic}),
    create_topics(I-1).


    

    

    

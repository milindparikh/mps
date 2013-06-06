-module(mps_pubsub). 
-export([subscribe/4, publish/3, unsubscribe/1, create_topics/0, create_topics/1]).
-export ([new_subscriber/0, add_subscription/2, delete_all_subscriptions/1]).


-define(REDUDANCY_FOR_SUBSCRIPTION, 0).
-define (TOPICRANGE, 2).
-define(NUMBEROFTOPICS,65535).
-define(BASE_FOR_ID, 62).

%% The main function that figures out the function to call on
%% the CallbackFun should be CallbackFun/4 with Instance, Topic, Key, Msg


%   {SubscriberId, 
%           [ {subscription, SubscriptionId, SubscriptionType, KExpr, KMatchExpr, [HTopics]},
%             {...}
%           ]
%   }



new_subscriber () ->
    { hd(flake_harness:generate(1, ?BASE_FOR_ID)), []}.

add_subscription (Subscriber, {Topic, KExpr, I, CallbackFun}) ->
    add_subscription (Subscriber, {Topic, KExpr, I, CallbackFun}, stream).


add_subscription ({SubscriberId, ListOfSubscriptions}, {Topic, KExpr, I, CallbackFun}, stream) ->
    SubscriptionId = flake_harness:generate(1, 62),
    HTopics = get_hex_topics(Topic,atom_to_list(stream), mps_utils:generate_keys_for_subscribe(KExpr)),
    SubscribedTopics = [{K, {mps_eventhandler_subscription, hd(flake_harness:generate(1, 62))}} || K <- HTopics],
    lists:foldl(fun ({T, {Module, Id}}, _A) ->
			gen_event:add_handler(T, {Module, Id}, [CallbackFun, I, Topic, KExpr])
		end,
		[],
		SubscribedTopics),
    
    Subscription = {subscription, hd(SubscriptionId), atom_to_list(stream), KExpr, mps_utils:generate_regexpr_for_word(KExpr), SubscribedTopics},

    {SubscriberId, [Subscription | ListOfSubscriptions]}.

delete_all_subscriptions(Subscriber) ->
    {SubscriberId, ListSubscriptions} = Subscriber,
    {SubscriberId, delete_subscriptions(ListSubscriptions)}.



delete_subscriptions([]) ->
    [];
delete_subscriptions([H|T]) -> 
    delete_particular_subscription(H),
    delete_subscriptions(T).

delete_particular_subscription({subscription, _Id, _Mode , _KExpr, _KMExpr, SubscribedTopics}) ->
    lists:foldl(fun ({EventMananger, {Module, Id}}, _A) ->
			gen_event:delete_handler(EventMananger, {Module, Id}, [] )
		end,
		[],
		SubscribedTopics),
    ok.


    
    

subscribe(Topic, KExpr, I, CallbackFun) ->
  subscribe(Topic, KExpr, I, CallbackFun, stream).

subscribe(Topic, KExpr, I, CallbackFun, stream) ->
    HTopics = get_hex_topics(Topic,atom_to_list(stream),  mps_utils:generate_keys_for_subscribe(KExpr)),
    
    SubscribedTopics = [{K, {mps_eventhandler_subscription, flake_harness:generate(1, 62)}} || K <- HTopics],

    %% THE FOLLOWING IS A NAIVE IMPLEMENTATION OF add_handler... USE SPAWN OR SOMETHING
    lists:foldl(fun ({T, {Mes, Id}}, _A) ->
			gen_event:add_handler(T, [Mes, Id], [CallbackFun, I, Topic, KExpr])
		end,
		[],
		SubscribedTopics),

    SubscribedTopics.


unsubscribe(SubscribedTopics) ->
    lists:foldl(fun ({T, {Mes, Id}}, A) ->
			[gen_event:delete_handler(T, [Mes, Id], [unsubscribe]) | A]
		end,
		[],
		SubscribedTopics).
    


publish(Topic, KExpr, Value) ->
    publish(Topic, KExpr, Value, stream).

publish(Topic, KExpr, Value, stream) -> 
    HTopics = get_hex_topics(Topic,atom_to_list(stream), mps_utils:generate_keys_for_publish(KExpr)),
    %% THE FOLLOWING IS A NAIVE IMPLEMENTATION OF NOTIFY... USE SPAWN OR SOMETHING
    lists:foldl(fun (X, _A) ->
			gen_event:notify(X, Value)
		end,
		[],
		HTopics),
    ok.





get_hex_topics (Topic, Mode, Keys) ->
    [hextopic(Topic++"-"++Mode++"-"++integer_to_list(N), Mode, K) || N <-lists:seq(0,?REDUDANCY_FOR_SUBSCRIPTION), K <- Keys].
    
hextopic(Topic, Mode, Key) ->    
    S = crypto:sha(Topic++Key),
    B = binary_to_list(S),
    {L1, _L2} = lists:split(?TOPICRANGE, B),
    
    list_to_atom("Topic-"++Mode++"-"++hexstring(list_to_binary(L1))).

    
hexstring(Binary) when is_binary(Binary) ->
    lists:flatten(lists:map(
        fun(X) -> io_lib:format("~2.16.0b", [X]) end, 
        binary_to_list(Binary))).


    
create_topics() ->
    create_topics(?NUMBEROFTOPICS, atom_to_list(stream)).

create_topics(Mode) ->
    create_topics(?NUMBEROFTOPICS, Mode).

create_topics(0, _Mode) ->
    ok;
create_topics(I, Mode) -> 
    B = <<I:16>>,

    A = "Topic-"++Mode++"-",
    
    HTopic =  list_to_atom(A++hexstring(B)),
    
    gen_event:start({local, HTopic}),
    create_topics(I-1, Mode).


    

    

    

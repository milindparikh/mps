-module(mps).
-include("mps.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         ping/0,
	 create_topics/0,
	 create_topics/1,
	 publish/3,
	 get_pid_for_topic/1
        ]).
-export ([new_subscriber/0, add_subscription/2, delete_all_subscriptions/1]).

%% Public API

%% @doc Pings a random vnode to make sure communication is functional
ping() ->
    DocIdx = riak_core_util:chash_key({<<"ping">>, term_to_binary(now())}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, mps),
    [{IndexNode, _Type}] = PrefList,
    io:format("Index Node ~p~n", [IndexNode]),
    riak_core_vnode_master:sync_spawn_command(IndexNode, ping, mps_vnode_master).


    
create_topics() ->
    create_topics(?NUMBEROFTOPICS, atom_to_list(stream)).

create_topics(Mode) ->
    create_topics(?NUMBEROFTOPICS, Mode).



publish(Topic, KExpr, Value) ->
    publish(Topic, KExpr, Value, stream).

publish(Topic, KExpr, Value, stream) -> 
    {ok, R} = re:compile(".*-.*-(.*)"),
    HTopics = mps_utils:get_hex_topics(Topic,atom_to_list(stream), mps_utils:generate_keys_for_publish(KExpr)),
    %% THE FOLLOWING IS A NAIVE IMPLEMENTATION OF NOTIFY... USE SPAWN OR SOMETHING
    lists:foldl(fun (X, _A) ->
			case X of 
			    undefined ->
				ok;
			    _ ->    
				case re:run(X, R) of 
				    {match, [{_, _}, {Fs, Fe}]} ->
					HTopic = lists:sublist(X, Fs+1, Fe),
					IndexNode = get_index_node(HTopic),
					riak_core_vnode_master:sync_spawn_command(
					  IndexNode, {publish, X, Value}, mps_vnode_master);
				    _ ->
					ok
				end
			end
		end,
		[],
		HTopics),
    ok.


new_subscriber () ->
    { hd(flake_harness:generate(1, ?BASE_FOR_ID)), []}.

add_subscription (Subscriber, {Topic, KExpr, I, CallbackFun}) ->
    add_subscription (Subscriber, {Topic, KExpr, I, CallbackFun}, stream).


add_subscription ({SubscriberId, ListOfSubscriptions}, {Topic, KExpr, I, CallbackFun}, stream) ->
    SubscriptionId = flake_harness:generate(1, 62),
    HTopics = mps_utils:get_hex_topics(Topic,atom_to_list(stream), mps_utils:generate_keys_for_subscribe(KExpr)),
    SubscribedTopics = [{K, {mps_eventhandler_subscription, hd(flake_harness:generate(1, 62))}} || K <- HTopics],

    lists:foldl(fun ({HTopic, {Module, Id}}, _A) ->
			IndexNode = get_index_node(HTopic),
			riak_core_vnode_master:sync_spawn_command(
			  IndexNode, 
			  {add_subscription, 
			   HTopic,
			   {Module, Id}, 
			   {[CallbackFun, I, Topic, KExpr]}},
			  mps_vnode_master )
			    
		end,
		[],
		SubscribedTopics),
    
    Subscription = {subscription, hd(SubscriptionId), atom_to_list(stream), KExpr, mps_utils:generate_regexpr_for_word(KExpr), SubscribedTopics},

    {SubscriberId, [Subscription | ListOfSubscriptions]}.
 
delete_all_subscriptions(Subscriber) ->
    {SubscriberId, ListSubscriptions} = Subscriber,
    {SubscriberId, delete_subscriptions(ListSubscriptions)}.













get_pid_for_topic(Topic) ->
    {ok, R} = re:compile(".*-.*-(.*)"),
    case re:run(Topic, R) of 
	{match, [{_, _}, {Fs, Fe}]} ->
	    HTopic = lists:sublist(Topic, Fs+1, Fe),
	    IndexNode = get_index_node(HTopic),
	    riak_core_vnode_master:sync_spawn_command(
	      IndexNode, {get_pid_for_topic, Topic}, mps_vnode_master);

	_ ->
	    ok
    end.









create_topic(Topic) ->
    {ok, R} = re:compile(".*-.*-(.*)"),
    case re:run(Topic, R) of 
	{match, [{_, _}, {Fs, Fe}]} ->
	    HTopic = lists:sublist(Topic, Fs+1, Fe),
	    IndexNode = get_index_node(HTopic),
	    riak_core_vnode_master:sync_spawn_command(
	      IndexNode, {create_topic, Topic}, mps_vnode_master),
	    ok;
	_ ->
	    ok
    end.



create_topics(0, _Mode) ->
    ok;
create_topics(I, Mode) -> 
    B = <<I:16>>,
    A = "Topic-"++Mode++"-",
    create_topic(   A++mps_utils:hexstring(B)),
    create_topics(I-1, Mode).


get_index_node(HTopic) ->    
    DocIdx = riak_core_util:chash_key({term_to_binary(HTopic), term_to_binary(HTopic) }),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, mps),
    [{IndexNode, _Type}] = PrefList,
    IndexNode.




	    

delete_subscriptions([]) ->
    [];
delete_subscriptions([H|T]) -> 
    delete_particular_subscription(H),
    delete_subscriptions(T).

delete_particular_subscription({subscription, _Id, _Mode , _KExpr, _KMExpr, SubscribedTopics}) ->

    lists:foldl(fun ({T, {Module, Id}}, _A) ->
			IndexNode = get_index_node(T),
			riak_core_vnode_master:sync_spawn_command(
			  IndexNode, {delete_subscription, T, {Module, Id}}, mps_vnode_master)
			    
		end,
		[],
		SubscribedTopics),
    
    ok.



    

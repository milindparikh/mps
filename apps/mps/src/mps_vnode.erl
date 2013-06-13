-module(mps_vnode).
-behaviour(riak_core_vnode).
-include("mps.hrl").

-export([start_vnode/1,
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_exit/3]).

-record(state, {partition, topartition, position, topics_in_vnode, num_topics}).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    
    NumOfPartitionsInRing = mps_utils:num_partitions_in_ring(),
    Position = trunc(((Partition/math:pow(2,160)) * NumOfPartitionsInRing)),
    ToPartition = Partition + trunc(math:pow(2,160)/NumOfPartitionsInRing) - 1,
    
    TopicsInVNode = topics_in_vnode(?NUMBEROFTOPICS, [], Partition, ToPartition),
    NumTopics = length(TopicsInVNode),

    {ok, #state { partition=Partition, 
		  position=Position ,
		  topartition=ToPartition,
		  topics_in_vnode = TopicsInVNode,
		  num_topics = NumTopics
		}
		  
    }.




% get_list_of_topics_for_this_vnode(Partition, ToPartition) ->
%    get_list_of_topics_for_this_vnode([], 0, ?NUMBEROFTOPICS, Partition, ToPartition).
%
% get_list_of_topics_for_this_vnode (ListTopics, _Count, 0, _Partition, _ToPartition) ->
%    ListTopics;
% get_list_of_topics_for_this_vnode (ListTopics, Count, Remaining, _Partition, _ToPartition) ->


handle_command(
  {add_subscription, 
   HTopic, 
   {Module, Id}, 
   {[CallbackFun, I, Topic, KExpr]}},
  _Sender, State) ->
    

%    io:format("In add subscription..."),
    
    case gproc:lookup_local_name(HTopic) of 
	undefined->
	    undefined;
	Pid -> 
	    gen_event:add_handler(gproc:get_value({n,l, HTopic}, Pid), 
				  {Module, Id}, [CallbackFun, I, Topic, KExpr])
    end,

%    io:format("....................done case~n"),


    {reply, {pid,
	     State#state.partition, 
	     State#state.position,
	     Topic
	     
	    }, 
     State};


%% Sample command: respond to a ping


handle_command(ping, _Sender, State) ->
    {reply, {pong, 
	     State#state.partition, 
	     State#state.position,
	     State#state.topartition,
	     State#state.num_topics

	    }, 
     State};



handle_command({publish, Topic, Value}, _Sender, State) ->


    case gproc:lookup_local_name(Topic) of 
	undefined->
	    undefined;
	Pid -> 
	    gen_event:notify(gproc:get_value({n,l, Topic}, Pid), Value)
    end,
    
    {reply, {pid,
	     State#state.partition, 
	     State#state.position

	     
	    }, 
     State};
                




handle_command({delete_subscription, 
		Topic, 
		{Module, Id}
	       },
	       _Sender, State) ->

    
    case gproc:lookup_local_name(Topic) of 
	undefined->
	    undefined;
	Pid -> 
	    gen_event:delete_handler(gproc:get_value({n,l, Topic}, Pid), 
				  {Module, Id}, [])
    end,
    
    {reply, {pid,
	     State#state.partition, 
	     State#state.position
	    }, 
     State};


handle_command({create_topic, Topic}, _Sender, State) ->
    mps_utils:reg_event_manager (Topic),
    
    {reply, {pid,
	     State#state.partition, 
	     State#state.position
	     
	    }, 
     State};

handle_command({get_pid_for_topic, Topic}, _Sender, State) ->
    SendPid = 
	case gproc:lookup_local_name(Topic) of 
	    undefined->
		undefined;
	    Pid -> 
		gproc:get_value({n,l, Topic}, Pid)
	end,

    {reply, {pid,
	     Topic,
	     SendPid
	    }, 
     State};

handle_command(Message, _Sender, State) ->
    ?PRINT({unhandled_command, Message}),
    {noreply, State}.

handle_handoff_command(_Message, _Sender, State) ->
    {noreply, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(_Data, State) ->
    {reply, ok, State}.

encode_handoff_item(_ObjectName, _ObjectValue) ->
    <<>>.

is_empty(State) ->
    {true, State}.

delete(State) ->
    {ok, State}.

handle_coverage({create_stream, Stream }, _KeySpaces, _Sender, State) -> 
    lists:foreach(fun ({E}) ->     

			  mps_utils:reg_event_manager ("Topic-"++Stream++"-"++E)
		  end,
		  State#state.topics_in_vnode),
    {noreply,  State};
handle_coverage(_Msg, _KeySpaces, _Sender, State) -> 
    {stop, notimplemented, State}.


handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.




topics_in_vnode (-1, ListTopics, _StartPartition, _EndPartition)->
    ListTopics;

topics_in_vnode (I, ListTopics, StartPartition, EndPartition)->
    B = <<I:16>>,
    C = mps_utils:hexstring(B),
    Index = riak_core_util:chash_key({term_to_binary(C), term_to_binary(C) }),
    <<IndexAsInt:160/integer>> = Index,
    case ((IndexAsInt >= StartPartition) and (IndexAsInt =< EndPartition)) of 
	true ->
	    topics_in_vnode(I-1, [{C}| ListTopics], StartPartition, EndPartition);
	false ->
	    topics_in_vnode(I-1,  ListTopics, StartPartition, EndPartition)
    end.

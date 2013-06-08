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

-record(state, {partition, position,start_range_topics, end_range_topics}).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    
    NumOfPartitionsInRing = num_partitions_in_ring(),
    Position = trunc(((Partition/math:pow(2,160)) * NumOfPartitionsInRing)),
    
    StartRangeTopics = trunc( ( ( (?NUMBEROFTOPICS + 1) * Position ) / NumOfPartitionsInRing )),
    EndRangeTopics = trunc( ( ( (?NUMBEROFTOPICS + 1) * (Position + 1) ) / NumOfPartitionsInRing ) - 1),
    
    
    
    
    {ok, #state { partition=Partition, 
		  position=Position ,
		  start_range_topics = StartRangeTopics,
		  end_range_topics = EndRangeTopics}
		  
    }.

%    {ok, #state { partition=Partition }}.





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
	     State#state.start_range_topics,
	     State#state.end_range_topics, Topic
	     
	    }, 
     State};


%% Sample command: respond to a ping


handle_command(ping, _Sender, State) ->
    {reply, {pong, 
	     State#state.partition, 
	     State#state.position,
	     State#state.start_range_topics,
	     State#state.end_range_topics

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
	     State#state.position,
	     State#state.start_range_topics,
	     State#state.end_range_topics, Topic
	     
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
	     State#state.position,
	     State#state.start_range_topics,
	     State#state.end_range_topics, Topic
	     
	    }, 
     State};


handle_command({create_topic, Topic}, _Sender, State) ->
    mps_utils:reg_event_manager (Topic),
    
    {reply, {pid,
	     State#state.partition, 
	     State#state.position,
	     State#state.start_range_topics,
	     State#state.end_range_topics, Topic
	     
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

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

num_partitions_in_ring() ->
    
    {ok, State} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring:num_partitions(State).

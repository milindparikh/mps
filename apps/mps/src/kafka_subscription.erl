%%%-------------------------------------------------------------------
%%% File     : kafka_client.erl
%%% Kafka Version 0.8   
%%% Author   : Milind Parikh <milindparikh@gmail.com>
%%%-------------------------------------------------------------------
 

-module(kafka_subscription).
-behaviour(gen_server).

-include("erlkafka.hrl").


-export ([locate_kafka_subscription/0, add_subscription/2]).
-export([start_link/0, start_link/2, shutdown/1]).

-export ([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-define(TIMER_INTERVAL, 1000).

%% 2^32  = 4294967296
%% fetch =          0
%% produce 1000000000
%% Metadata2000000000
%% OffsetF 3000000000
%% FetchC  4000000000


%% KAFKA METADATA RESPONSE 
%%        [
%%           [
%%              {1, '127.0.0.1', 9092},
%%              {2, '127.0.0.1', 9093},
%%              {3, '127.0.0.1', 9094},
%%              {4, '127.0.0.1', 9095}
%%           ],
%%           [
%%              {"Topic1, [
%%                          {1, 1, [2,3,4], [2,4]},
%%                          {2, 3, [1,2,4], [2,4]},
%%                          {3, 4, [1,2,3], [1,2]},
%%                          {4, 2, [1,3,4], [1,4]}
%%                        ]
%%              }
%%           ]
%%        ]
%%           
%%
%%                          
%%           

-record(state, { list_topics = [], hardcodedoffset=0, kafkaclientpid , publish=0
                }).



%%%-------------------------------------------------------------------
%%%                         API FUNCTIONS
%%%-------------------------------------------------------------------


locate_kafka_subscription () ->
    {_, _, Pid} =     hd(lists:sublist( hd(gproc:select([{{'_', '_', kafka_subscription}, [], ['$$']}])), 1,1)),
    Pid.
add_subscription (Pid, {Topic, Stream, Fun}) ->
    gen_server:call(Pid, {add_subscription, {Topic, Stream, Fun}}). 



start_link() ->
    gen_server:start_link( ?MODULE, [], []).

start_link(in_mps, Mode) ->
    gen_server:start_link( ?MODULE, [in_mps, Mode], []).

shutdown(Pid) ->    
    Pid ! shutdown.


 
    


%%%-------------------------------------------------------------------
%%%                         GEN_SERVER CB FUNCTIONS
%%%-------------------------------------------------------------------


init([]) -> 
    erlang:send_after(?TIMER_INTERVAL, self(), timer),
    {ok, #state{}, 0};



init([in_mps, publish]) -> 
    
    gproc:reg({p, l, self()}, kafka_subscription),
    {ok, Pid } = kafka_client:start_link(),
    
    erlang:send_after(?TIMER_INTERVAL, self(), timer),
    {ok, #state{kafkaclientpid = Pid, publish=1}, 0};



init([in_mps, print]) -> 
    
    gproc:reg({p, l, self()}, kafka_subscription),
    {ok, Pid } = kafka_client:start_link(),
    
    erlang:send_after(?TIMER_INTERVAL, self(), timer),
    {ok, #state{kafkaclientpid = Pid, publish=0}, 0}.


handle_call({add_subscription, {_Topic, _Stream, _Fun}}, _From, State) ->  
% JUST A PLACE HOLDER FOR NOW
    {reply, ok, State};

   

handle_call(_Request, _From, State) ->
    {reply, [], State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info (timer, State) ->



    Pid = State#state.kafkaclientpid,
    
    CorrId = kafka_client:request_fetch(Pid, [{<<"Topic1">>, [   {0, State#state.hardcodedoffset, 20000 }]}]),     % one topic,one partition fetching at offset 0
    timer:sleep(2000),
    T = kafka_client:response_fetch(Pid, CorrId),
    
    {_, _, { [ {_, [ [{_, _, _, KList}]]} ] }} = T,
    
    case KList == <<"">> of 
	false ->
	    LastOffSet = 
		lists:foldl(fun ({OffSet,_, _, Key, Value}, _A) ->
				    case State#state.publish == 1 of 
					true ->
					    mps:publish (binary_to_list(<<"Topic1">>), binary_to_list(Key), binary_to_list(Value));
					false ->
					    io:format("~p,~p,~p~n", ["Topic1", binary_to_list(Key), binary_to_list(Value)])
				    end,
				    OffSet
			    end,
			    {},
			    lists:reverse(KList)),
	    
	    io:format("~p~n", [LastOffSet]),
	    erlang:send_after(?TIMER_INTERVAL, self(), timer),
	    {noreply, State#state{hardcodedoffset = LastOffSet + 1}};
	true ->
	    erlang:send_after(?TIMER_INTERVAL, self(), timer),
	    {noreply, State}
    end;

handle_info(shutdown, State) -> 
  {stop, shutdown, State};

handle_info(_, State) -> 
  {noreply, State}.


terminate(_Reason, _State) ->
     ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


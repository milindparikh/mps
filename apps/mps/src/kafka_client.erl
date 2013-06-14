%%%-------------------------------------------------------------------
%%% File     : kafka_client.erl
%%% Kafka Version 0.8   
%%% Author   : Milind Parikh <milindparikh@gmail.com>
%%%-------------------------------------------------------------------
 

-module(kafka_client).
-behaviour(gen_server).

-include("erlkafka.hrl").

-export ([start_link/0, start_link/1, request_metadata/2, response_metadata/2, request_produce/2, request_produce_ignore/2, response_produce/2, request_fetch/2, response_fetch/2, request_offset/2, response_offset/2]).
-export ([locate_kafka_client/0]).

-export ([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-define(TIMER_INTERVAL, 1000).
-define(BLOOMER_INTERVAL, 10000).
-define(BLOOM_CAPACITY, 20000).

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

-record(state, {socket,  bloomstage0, bloomstage1, leftover=nill, list_responses = [],
		start_metadata_cid   = ?STARTMD,
		start_produce_cid    = ?STARTPR,
		start_fetch_cid      = ?STARTFE,
		start_offsetr_cid    = ?STARTOR,
                start_offsetf_cid    = ?STARTOF,
                start_offsetc_cid    = ?STARTOC}).



%%%-------------------------------------------------------------------
%%%                         API FUNCTIONS
%%%-------------------------------------------------------------------


locate_kafka_client () ->
    {_, _, Pid} =     hd(lists:sublist( hd(gproc:select([{{'_', '_', kafka_client}, [], ['$$']}])), 1,1)),
    Pid.

start_link() ->
    start_link( ['127.0.0.1', 9092]).


start_link(in_mps) ->
    start_link( ['127.0.0.1', 9092, in_mps]);

start_link([Host, Port]) -> 
   gen_server:start_link( ?MODULE, [Host, Port], []);

start_link([Host, Port, in_mps]) -> 
   gen_server:start_link( ?MODULE, [Host, Port, in_mps], []).

request_metadata(Pid, Topics) -> 
 	gen_server:call(Pid, {request_metadata, Topics}).

response_metadata(Pid, CorrId)->
    
    gen_server:call(Pid, {response_metadata, CorrId}).



request_produce(Pid, Topics) -> 
    gen_server:call(Pid, {request_produce, Topics}).

request_produce_ignore(Pid, Topics) -> 
    gen_server:call(Pid, {request_produce_ignore, Topics}).

response_produce(Pid, CorrId)->
    gen_server:call(Pid, {response_produce, CorrId}).



request_fetch(Pid, Topics) -> 
 	gen_server:call(Pid, {request_fetch, Topics}).

response_fetch(Pid, CorrId)->
%    io:format("RESPONSE FETCH ~p~n", [CorrId]),
    gen_server:call(Pid, {response_fetch, CorrId}).




request_offset(Pid, Topics) -> 
 	gen_server:call(Pid, {request_offset, Topics}).

response_offset(Pid, CorrId)->
    gen_server:call(Pid, {response_offset, CorrId}).

    
 
    


%%%-------------------------------------------------------------------
%%%                         GEN_SERVER CB FUNCTIONS
%%%-------------------------------------------------------------------


init([Host, Port]) -> 
    {ok, Socket} = gen_tcp:connect(Host, Port,
                                   [binary, {active, once}, {packet, raw}]),
    
    erlang:send_after(?TIMER_INTERVAL, self(), timer),
    erlang:send_after(?BLOOMER_INTERVAL, self(), replace_bloom_filters),
    {ok, 
     #state{socket=Socket, 
	    bloomstage0 = bloom:bloom(?BLOOM_CAPACITY),
	    bloomstage1 = bloom:bloom(?BLOOM_CAPACITY)
	   }, 
     0};



init([Host, Port, in_mps]) -> 
    {ok, Socket} = gen_tcp:connect(Host, Port,
                                   [binary, {active, once}, {packet, raw}]),
    
    gproc:reg({p, l, self()}, kafka_client),
    
    erlang:send_after(?TIMER_INTERVAL, self(), timer),
    {ok, 
     #state{socket=Socket, 
	    bloomstage0 = bloom:bloom(?BLOOM_CAPACITY),
	    bloomstage1 = bloom:bloom(?BLOOM_CAPACITY)
	   }, 

     0}.


handle_call({response_metadata, CorrId}, _From, State) -> 
	    
    case lists:keyfind(CorrId, 1, State#state.list_responses) of 
	false ->
	    NewState = State,
	    {reply, false, NewState};
	Tuple ->
	    NewResponseList = lists:keydelete(CorrId, 1, State#state.list_responses),
	    NewState = State#state{start_metadata_cid= CorrId + 1, list_responses=NewResponseList},
	    {reply, Tuple, NewState}
    end;

handle_call({request_metadata, Topics}, _From, State) -> 
    CorrId = State#state.start_metadata_cid,
    ClientId = "ERLKAFKA",
    
    RequestData = kafka_protocol:metadata_request(CorrId, ClientId, Topics),
    
    Socket = State#state.socket,
    case gen_tcp:send(Socket, RequestData) of 
	ok ->
	    {reply, CorrId, State#state{start_metadata_cid = CorrId+1}};

	{error, Reason} ->
	    {reply, {error, Reason}, State}
    end;




handle_call({response_produce, CorrId}, _From, State) -> 
	    
    case lists:keyfind(CorrId, 1, State#state.list_responses) of 
	false ->
	    NewState = State,
	    {reply, false, NewState};
	Tuple ->
	    NewResponseList = lists:keydelete(CorrId, 1, State#state.list_responses),
	    NewState = State#state{start_produce_cid= CorrId + 1, list_responses=NewResponseList},
	    {reply, Tuple, NewState}
    end;

handle_call({request_produce, Topics}, _From, State) -> 
    CorrId = State#state.start_produce_cid,
    ClientId = "ERLKAFKA",
    
    RequestData = kafka_protocol:produce_request(CorrId, ClientId, Topics),
    
    Socket = State#state.socket,
    case gen_tcp:send(Socket, RequestData) of 
	ok ->
	    {reply, CorrId, State#state{start_produce_cid = CorrId+1}};

	{error, Reason} ->
	    {reply, {error, Reason}, State}
    end;

handle_call({request_produce_ignore, Topics}, _From, State) -> 
    CorrId = State#state.start_produce_cid,
    ClientId = "ERLKAFKA",
    UpdatedB1 = bloom:add_element(CorrId, State#state.bloomstage1),
    
    RequestData = kafka_protocol:produce_request(CorrId, ClientId, Topics),
    
    Socket = State#state.socket,
    case gen_tcp:send(Socket, RequestData) of 
	ok ->
	    {reply, CorrId, State#state{start_produce_cid = CorrId+1, bloomstage1=UpdatedB1}};

	{error, Reason} ->
	    {reply, {error, Reason}, State}
    end;







handle_call({response_fetch, CorrId}, _From, State) -> 
    
%    io:format("IN GEN SERVER FETCH ~p~n", [CorrId]),
    case lists:keyfind(CorrId, 1, State#state.list_responses) of 
	false ->
	    NewState = State,
	    {reply, false, NewState};
	Tuple ->
	    NewResponseList = lists:keydelete(CorrId, 1, State#state.list_responses),
	    NewState = State#state{start_fetch_cid= CorrId + 1, list_responses=NewResponseList},
	    {reply, Tuple, NewState}
    end;

handle_call({request_fetch, Topics}, _From, State) -> 
    CorrId = State#state.start_fetch_cid,
    ClientId = "ERLKAFKA",
    
    RequestData = kafka_protocol:fetch_request(CorrId, ClientId, Topics),
    
    Socket = State#state.socket,
    case gen_tcp:send(Socket, RequestData) of 
	ok ->
	    {reply, CorrId, State#state{start_fetch_cid = CorrId+1}};

	{error, Reason} ->
	    {reply, {error, Reason}, State}
    end;




handle_call({response_offset, CorrId}, _From, State) -> 

    case lists:keyfind(CorrId, 1, State#state.list_responses) of 
	false ->
	    NewState = State,
	    {reply, false, NewState};
	Tuple ->
	    NewResponseList = lists:keydelete(CorrId, 1, State#state.list_responses),
	    NewState = State#state{start_offsetr_cid= 0, list_responses=NewResponseList},  %% TO DO : FIX AFTER THE KAFKA OFFSET REQUEST IS FIXED TO RETURN A PROPER COORDID
	    {reply, Tuple, NewState}
    end;

handle_call({request_offset, Topics}, _From, State) -> 
    CorrId = 0,                                                                   %% TO DO : FIX AFTER THE KAFKA OFFSET REQUEST IS FIXED TO RETURN A PROPER COORDID
    ClientId = "ERLKAFKA",
    
    RequestData = kafka_protocol:offset_request(CorrId, ClientId, Topics),
    
    Socket = State#state.socket,
    case gen_tcp:send(Socket, RequestData) of 
	ok ->
	    {reply, 0 , State#state{start_offsetr_cid = 0}};                       %% TO DO : FIX AFTER THE KAFKA OFFSET REQUEST IS FIXED TO RETURN A PROPER COORDID

	{error, Reason} ->
	    {reply, {error, Reason}, State}
    end;

   

handle_call(_Request, _From, State) ->
    {reply, [], State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info (timer, State) ->
    Socket = State#state.socket,
    inet:setopts(Socket, [{active, once}]),
    erlang:send_after(?TIMER_INTERVAL, self(), timer),
    {noreply, State};

handle_info (replace_bloom_filters, State) ->
    B1= bloom:bloom(?BLOOM_CAPACITY),
    B0 = State#state.bloomstage1,   % This is not a typo
    erlang:send_after(?BLOOMER_INTERVAL, self(), timer),
    {noreply, State#state{bloomstage0=B0, bloomstage1=B1}};




handle_info({tcp, Socket, Data}, #state{leftover=Prepend} = State) when Prepend /= nill ->

    {ok, NewState} = handle_streaming_data(<<Prepend/binary, Data/binary>>, State),
    inet:setopts(Socket, [{active, once}]),
    {noreply, NewState};

handle_info({tcp, Socket, Data}, State) ->

    {ok, NewState} = handle_streaming_data(Data, State),
    inet:setopts(Socket, [{active, once}]),
    {noreply, NewState};

handle_info({tcp_closed, _Socket}, State) ->
    io:fwrite("irlpact_connector:handle_info tcp_close~n", []),
    {noreply, State};

handle_info({tcp_error, _Socket, Reason}, State) ->
    io:fwrite("irlpact_connector:handle_info tcp_error : ~s~n", [Reason]),
    {noreply, State};


handle_info(_, State) -> 
  {noreply, State}.


terminate(_Reason, _State) ->
     ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%-------------------------------------------------------------------
%%%                         INTERNAL FUNCTIONS
%%%-------------------------------------------------------------------
    
handle_streaming_data(<<>>, State) -> 
%    io:format("GEN SERVER2 ~p~n", [State#state.list_responses]),
   {ok, State};

handle_streaming_data( Data, State) ->
    
%    io:format("Data to be consumed  ~p~n", [State#state.leftover]),
    {LengthConsumed, ListOfResponses} = kafka_protocol:parse_response_stream(Data),
    
    TruncatedListOfResponse1 = [X || X <- ListOfResponses, not(bloom:is_element(X, State#state.bloomstage0))],
    TruncatedListOfResponse2 = [X || X <- TruncatedListOfResponse1, not(bloom:is_element(X, State#state.bloomstage1))],
    
    case LengthConsumed > 0 of 
	true ->
	    <<_:LengthConsumed/binary,  Rest/bitstring>> = Data,
	    
	    NewResponseList = lists:flatten([TruncatedListOfResponse2 | State#state.list_responses]),
	    NewState = State#state{leftover=Rest, list_responses=NewResponseList},
	    handle_streaming_data(Rest, NewState);
	false ->
	    NewState = State#state{leftover=Data},
%	    io:format("Nothing was consumede from ~p~n", [NewState#state.leftover]),
	    {ok, NewState}
    end.




				 


    

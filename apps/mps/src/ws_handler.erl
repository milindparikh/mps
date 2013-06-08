-module(ws_handler).
%% This module is  the logical endpoint of the delivery of subscribed messages. 
%% to the browser
%%
%% As such, this is logically where the what to do with the redudancy of the mesage 
%% delivery should be dealt with if the semantic necessity of once-and-only-once
%% delivery is necessary in context of mps's redudant message delivery.
%%
%% At this layer, mps gurantees that only the correct topic & key are delivered; 
%% regardless of the underlying gen_event layer that drives this


-behaviour(cowboy_websocket_handler).

-export([init/3]).
-export([websocket_init/3]).
-export([websocket_handle/3]).
-export([websocket_info/3]).
-export([websocket_terminate/3]).
-export([mps_subscription_fun/4]).
-record(state, {subscriber}).

mps_subscription_fun (Instance, Topic, Key, Msg) ->
%    io:format("In ws_handler:mps_subscription_fun  debug ~p ~p ~p~n", [Topic, Key, Msg]),
    Instance ! {timeout, Instance, {Topic, Key, Msg}}.    %% THIS SHOULD BE CHANGED TO ACCOMODATE REDUNDANCY


init({tcp, http}, _Req, _Opts) ->
	{upgrade, protocol, cowboy_websocket}.

websocket_init(_TransportName, Req, _Opts) ->
    NewSubscriber = mps:new_subscriber(),
    {ok, Req, #state{subscriber=NewSubscriber}}.


%% This is where the subscribe  from browser occurs 
%% Be careful about code injection at this level 
%% 
websocket_handle({text, Msg2}, Req, State) ->
    {ok, Rp} = re:compile("publish{(.*)\,\s*(.*)\,\s*(.*)}"),
    {ok, Rs} = re:compile("subscribe{(.*)\,\s*(.*)}"),

    Msg = binary_to_list(Msg2),
    
    case re:run(Msg, Rp) of 
	{match, [{_,_}, {Ts, Te}, {Ks, Ke}, {Vs, Ve}]} ->
%	    io:format("In publish debug ~p ~p ~p~n", [lists:sublist(Msg, Ts+1, Te), lists:sublist(Msg, Ks+1, Ke), lists:sublist(Msg, Vs+1, Ve)]),
	    mps:publish(lists:sublist(Msg, Ts+1, Te), lists:sublist(Msg, Ks+1, Ke), lists:sublist(Msg, Vs+1, Ve)),
	    NewState = State;
	_ -> 
	    case re:run(Msg, Rs) of 

		{match, [{_,_}, {Ts, Te}, {Ks, Ke}]} -> 
%		    io:format("In subscribe debug ~p ~p~n", [lists:sublist(Msg, Ts+1, Te), lists:sublist(Msg, Ks+1, Ke)]),
		    ChangedSubscriber = mps:add_subscription(State#state.subscriber, {lists:sublist(Msg, Ts+1, Te), 
											     lists:sublist(Msg, Ks+1, Ke), 
											     self(), 
											     fun ws_handler:mps_subscription_fun/4}),
		    NewState = State#state{subscriber = ChangedSubscriber};    
		nomatch -> 
		    NewState = State
	    end
    end,
    {ok, Req, NewState};

websocket_handle(_Data, Req, State) ->
	{ok, Req, State}.


%% This is where the actual value gets propogated back to the browser . 
%% so the checks for duplicate messages, if necessary, are done here
%% and any ack back

websocket_info({timeout, _Ref, {_Topic, _Key, Msg}}, Req, State) ->
%    io:format("In ws_handler:websocket_info  debug ~p ~p ~p~n", [Topic, Key, Msg]),
    {reply, {text, Msg}, Req, State};
websocket_info(_Info, Req, State) ->
    {ok, Req, State}.

websocket_terminate(_Reason, _Req, _State) ->
    ok.

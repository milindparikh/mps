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
-record(state, {prev_subscription}).

empty_state() ->
    #state{}.

mps_subscription_fun (Instance, Topic, Key, Msg) ->
    Instance ! {timeout, Instance, {Topic, Key, Msg}}.    %% THIS SHOULD BE CHANGED TO ACCOMODATE REDUNDANCY


init({tcp, http}, _Req, _Opts) ->
	{upgrade, protocol, cowboy_websocket}.

websocket_init(_TransportName, Req, _Opts) ->
%%	erlang:start_timer(1000, self(), <<"Hello!">>),
%%      The statement below is HARDCODED Topic1
%%          Actually should be handled at the websocket_handle level

    mps_pubsub:subscribe("Topic1", "Key1", self(), fun ?MODULE:mps_subscription_fun/4),
    {ok, Req, empty_state()}.


%% This is where the subscribe  from browser occurs 
%% Be careful about code injection at this level 
%% 
websocket_handle({text, Msg}, Req, State) ->
    {ok, Rp} = re:compile("publish{(.*)\,\s*(.*)\,\s*(.*)}"),
    {ok, Rs} = re:compile("subscribe{(.*)\,\s*(.*)}"),

    case re:run(Msg, Rp) of 
	{match, [{_,_}, {Ts, Te}, {Ks, Ke}, {Vs, Ve}]} ->
	    mps_pubsub:publish(lists:sublist(Msg, Ts, Te), lists:sublist(Msg, Ks, Ke), lists:sublist(Msg, Vs, Ve)),
	    NewState = State;
	_ -> 
	    case re:run(Msg, Rs) of 
		{match, [{_,_}, {Ts, Te}, {Ks, Ke}]} -> 
		    case State#state.prev_subscription of 
			undefined ->
			    SubscribedTopics = mps_pubsub:subscribe(lists:sublist(Msg, Ts, Te), lists:sublist(Msg, Ks, Ke), self(), fun ws_handler:mps_subscription/4),
			    NewState = State#state{prev_subscription = SubscribedTopics};
			_ -> 
			    mps_pubsub:unsubscribe(State#state.prev_subscription),
			    SubscribedTopics = mps_pubsub:subscribe(lists:sublist(Msg, Ts, Te), lists:sublist(Msg, Ks, Ke), self(), fun ws_handler:mps_subscription/4),
			    NewState = State#state{prev_subscription = SubscribedTopics}
		    end;
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
	{reply, {text, Msg}, Req, State};
websocket_info(_Info, Req, State) ->
	{ok, Req, State}.

websocket_terminate(_Reason, _Req, _State) ->
	ok.

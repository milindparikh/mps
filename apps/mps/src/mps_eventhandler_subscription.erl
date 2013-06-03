-module(mps_eventhandler_subscription).

-behaviour(gen_event).
-export([init/1, handle_event/2, handle_info/2, handle_call/2, code_change/3, terminate/2]).
-record (state, {function, instance, topic, key}).

init([F,I, T, K]) ->
    State =  #state{
      function = F,
      instance=I,
      topic = T,
      key = K},
    {ok, State}.

handle_event(Msg, State) ->
    F = State#state.function,
    T = State#state.topic,
    I = State#state.instance,
    K = State#state.key,
    F(I, T, K, Msg),
    {ok, State}.

handle_info({'EXIT', _Pid, _Reason}, State) ->
    {ok, State}.

handle_call(_Args, State) ->
    {ok, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Args, _State) ->
    ok.

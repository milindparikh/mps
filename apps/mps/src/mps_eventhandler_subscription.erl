-module(mps_eventhandler_subscription).

-behaviour(gen_event).
-export([init/1, handle_event/2, handle_info/2, handle_call/2, code_change/3, terminate/2]).
-record (state, {function, instance, topic, key, regexpr}).

init([F,I, T, K]) ->
    {ok, R} = re:compile(K),
    
    State =  #state{
      function = F,
      instance=I,
      topic = T,
      key = K,
      regexpr = R
     },
    {ok, State}.

handle_event({Topic, Key, Msg}, State) ->
    F = State#state.function,
    T = State#state.topic,
    I = State#state.instance,
    K = State#state.key,
    R = State#state.regexpr,
    
    case Topic == T of
	true ->
	    case re:run (Key, R) of 
		{match, _} ->
		    F(I, T, K, Msg),
		    {ok, State};
		_ ->
		    {ok, State}
	    end;
	false ->
	    {ok, State}
    end.


handle_info({'EXIT', _Pid, _Reason}, State) ->
    {ok, State}.

handle_call(_Args, State) ->
    {ok, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Args, _State) ->
    ok.

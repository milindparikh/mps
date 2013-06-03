-module(mps_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    case mps_sup:start_link() of
        {ok, Pid} ->
            ok = riak_core:register([{vnode_module, mps_vnode}]),
            ok = riak_core_ring_events:add_guarded_handler(mps_ring_event_handler, []),
            ok = riak_core_node_watcher_events:add_guarded_handler(mps_node_event_handler, []),
            ok = riak_core_node_watcher:service_up(mps, self()),
	    websocket:start(),
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok.

-module(mps_event_handler_simple).
-export([mps_subscription_fun/4]).

mps_subscription_fun (_Instance, Topic, Key, Msg) ->
    io:format("GOT EVENT ~w~n", [[Topic, Key, Msg]]).


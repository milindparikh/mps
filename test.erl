-module(test).
-export([test/0]).

test() ->
    io:format("creating topics..."),
    mps:create_topics(),
    io:format("..done"),
    
    io:format("subscribing to keys.."),
    S = mps:new_subscriber(),
    S1 = mps:add_subscription(S, {"Topic1", "Key1", some, fun mps_event_handler_simple:mps_subscription_fun/4}),
    io:format("..done"),
    io:format("~p~n", [S1]),
    
    io:format("publisiing events..."),
    mps:publish("Topic1", "Key1", "Value1"),
    io:format("...done").

    

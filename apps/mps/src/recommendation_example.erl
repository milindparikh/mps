-module(recommendation_example).
-export([generate_data/1]).

generate_data(N) ->
    {A1, A2, A3} = now(),
    random:seed(A1, A2, A3),
    List = [ random:uniform(100) ||          _K <- lists:seq(1,N)],
    
    lists:foldl(fun (X, _A) ->
			User = list_to_binary("loggedin:user"++integer_to_list(X)),
			mps:publish_to_kafka_ignore(<<"Topic1">>,  User, <<"T">>),
			io:format("~p~n", [User])
		end,
		[], 
		List).

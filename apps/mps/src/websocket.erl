%% Feel free to use, reuse and abuse the code in this file.

-module(websocket).

%% API.
-export([start/0]).

start() ->

	Dispatch = cowboy_router:compile([
		{'_', [
			{"/", toppage_handler, []},
		       {"/publish", pubpage_handler, []},
			{"/websocket", ws_handler, []},
			{"/static/[...]", cowboy_static, [
							  {directory, {priv_dir, mps, [<<"static">>]}},
						%				{mimetypes, {fun mimetypes:path_to_mimes/2, default}}
							  {mimetypes, [
								       {<<".css">>, [<<"text/css">>]},
								       {<<".js">>, [<<"application/javascript">>]}]}

			]}
		]}
	]),
	{ok, _} = cowboy:start_http(http, 100, [{port, 8080}],
		[{env, [{dispatch, Dispatch}]}]).

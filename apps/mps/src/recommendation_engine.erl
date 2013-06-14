%%%-------------------------------------------------------------------
%%% File     : kafka_client.erl
%%% Kafka Version 0.8   
%%% Author   : Milind Parikh <milindparikh@gmail.com>
%%%-------------------------------------------------------------------
 

-module(recommendation_engine).
-behaviour(gen_server).

-include("erlkafka.hrl").



-export([start_link/0, start_link/1 , shutdown/1]).
-export([make_recommendation_fun/4]).
-export ([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, { subscriber,products
                }).



%%%-------------------------------------------------------------------
%%%                         API FUNCTIONS
%%%-------------------------------------------------------------------

make_recommendation_fun (Instance, Topic, Key, Msg) ->
    Instance ! {make_recommendation, Topic, Key, Msg}.


start_link() ->
    gen_server:start_link( ?MODULE, [], []).

start_link(ListUsers) ->
    gen_server:start_link( ?MODULE, [ListUsers], []).

shutdown(Pid) ->    
    Pid ! shutdown.


 
    


%%%-------------------------------------------------------------------
%%%                         GEN_SERVER CB FUNCTIONS
%%%-------------------------------------------------------------------



init([]) -> 
    NewSubscriber = mps:new_subscriber(),
    Products =  ["ROUNDBRAN", "CORNBRAN", "WHEATBRAN", "OATBRAN", "RICEBRAN"],
    {A1, A2, A3} = now(),
    random:seed(A1, A2, A3),
    {ok, #state{
       subscriber=NewSubscriber, 
       products=Products}};

init([ListUsers]) -> 
    NewSubscriber = mps:new_subscriber(),
    Products =  ["ROUNDBRAN", "CORNBRAN", "WHEATBRAN", "OATBRAN", "RICEBRAN"],
    
    ChangedSubscriber = add_subscription(length(ListUsers), NewSubscriber, "Topic1", ListUsers, self()), 
    {A1, A2, A3} = now(),
    random:seed(A1, A2, A3),
    {ok, #state{
       subscriber=ChangedSubscriber, 
       products=Products}}.
    

add_subscription(0, Subscriber,_Topic, _ListUsers, _Pid) ->
    Subscriber;
add_subscription(N, Subscriber, Topic, ListUsers, Pid) ->
    add_subscription( N-1, mps:add_subscription(Subscriber, {Topic, 
							     "loggedin:"++lists:nth(N, ListUsers), 
							     Pid, 
							     fun recommendation_engine:make_recommendation_fun/4}
					       ),
		      Topic, ListUsers, Pid).
    

handle_call(_Request, _From, State) ->
    {reply, [], State}.

handle_cast(_Request, State) ->
    {noreply, State}.


handle_info({make_recommendation, Topic, Key, _Msg}, State) ->
    {ok, R} = re:compile("loggedin:(.*)"),
    {match, [{_, _}, {StartM, EndM}]} = re:run(Key, R),
    User = lists:sublist(Key, StartM+1, EndM),
    Product = lists:nth(random:uniform(5), State#state.products),
    UserKey = "user:"++ User ++":recommendation",
    

    mps:publish(Topic, UserKey, Product),
    io:format("mps:publish ( ~p, ~p, ~p~n )", [Topic, UserKey, Product]),
    {noreply, State};

handle_info(shutdown, State) -> 
  {stop, shutdown, State};

handle_info(_, State) -> 
  {noreply, State}.


terminate(_Reason, _State) ->
     ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


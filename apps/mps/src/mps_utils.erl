-module(mps_utils).
-include("mps.hrl").

-export([generate_keys_for_publish/1]).
-export([generate_keys_for_subscribe/1]).
-export([generate_regexpr_for_word/1]).
-export ([get_hex_topics/3]).
-export ([hexstring/1]).
-export([find_vnode_for_hex_topic/1]).
-export([reg_event_manager/1, lookup_event_manager/1]).
-export([num_partitions_in_ring/0]).


generate_regexpr_for_word(Word) ->
    
    {ok, RExpr} = re:compile(re:replace(Word, "\\.", "\\\\.", [{return, list}, global])),
    RExpr.



%% "com.amazon.music"   -> ["com.amazon.music"]
%% "com.amazon.*"   -> ["com.amazon"]
%% "com.(amazon|ebay).*"   -> ["com.amazon", "com.ebay" ]


generate_keys_for_subscribe(Word) ->
    SplitWords = re:split(Word, "\\."),
    {ok, RExpr } = re:compile("\\*"),
    
    L = length(SplitWords),

    PositionOfStar = lists:foldl (fun (X, CurrentPos)  ->
					  case re:run(lists:nth(X, SplitWords), RExpr) of 
					      {match, _ } ->
						  X;
					      nomatch -> 
						  CurrentPos
					  end
				  end,
				  -1,
				  lists:seq(1, L)),
    case PositionOfStar of 
	-1 ->
	    generate_keys(SplitWords, 1, L);
	_ -> 	    
	    generate_keys(SplitWords, 1, PositionOfStar-1)
				  
    end.



%% "com.amazon.music"   -> ["com", "com.amazon", "com.amazon.music"]
%% "com.(amazon|ebay).music"  -> ["com","com.amazon","com.ebay","com.amazon.music", "com.ebay.music"]


generate_keys_for_publish(Word) ->
    SplitWords = re:split(Word, "\\."),
    L = length(SplitWords),

    lists:foldl(fun (X, A) ->
			lists:append(A, generate_keys(SplitWords, 1, X))
		end,
		[],
		lists:seq(1, L)).




generate_keys (SplitWords, 1, Count) ->
    {ok, RExpr} = re:compile("\\(.*\\)"),

    WordInQ = lists:nth(1      , SplitWords),
    
    
    case re:run(WordInQ, RExpr) of 
	nomatch ->
	    generate_keys(SplitWords, 2, Count -1, [binary_to_list(WordInQ)]);
	{match, [{ Start, LengthMatch}]}   ->
	    
	    SubList = lists:sublist(binary_to_list(WordInQ), Start + 2, LengthMatch - 2),
	    SplitSubList =  re:split(SubList, "\\|"),
	    generate_keys(SplitWords, 2, Count -1, lists:foldl(fun(X, A) ->
									       lists:append(A, [binary_to_list(X)])
								       end,
								       [],
								       SplitSubList))
    end;




generate_keys (SplitWords, StartAt, _Count) ->
    {ok, RExpr} = re:compile("\\(.*\\)"),

    WordInQ = lists:nth(StartAt, SplitWords),
    
    
    case re:run(WordInQ, RExpr) of 
	nomatch ->
	    [binary_to_list(WordInQ)];
	{match, [{ Start, LengthMatch}]}   ->
	    
	    SubList = lists:sublist(binary_to_list(WordInQ), Start + 2, LengthMatch - 2),
	    SplitSubList =  re:split(SubList, "\\|"),
	    lists:foldl(fun(X, A) ->
				lists:append(A, [binary_to_list(X)])
			end,
			[],
			SplitSubList)
    end.

generate_keys (_SplitWords, _StartAt, 0, List ) ->
    List;


generate_keys (SplitWords, StartAt, Count, List ) ->
    generate_keys ( SplitWords, StartAt+1, Count-1, [L1++"."++A1 || L1 <- List, A1 <- generate_keys(SplitWords, StartAt, Count)]).



find_vnode_for_hex_topic (HTopic) ->
    trunc(list_to_integer(lists:flatten(lists:map(fun (X) -> io_lib:format("~2.16.0b", [X]) end, HTopic)), 16)/?NUMBEROFTOPICS).




get_hex_topics (Topic, Mode, Keys) ->
    [{Topic, K, hextopic(Topic++"-"++Mode++"-"++integer_to_list(N), Mode, K)} || N <-lists:seq(0,?REDUDANCY_FOR_SUBSCRIPTION), K <- Keys].


    
hextopic(Topic, Mode, Key) ->    
    S = crypto:sha(Topic++Key),
    B = binary_to_list(S),
    {L1, _L2} = lists:split(?TOPICRANGE, B),
    "Topic-"++Mode++"-"++hexstring(list_to_binary(L1)).



    
hexstring(Binary) when is_binary(Binary) ->
    lists:flatten(lists:map(
		    fun(X) -> io_lib:format("~2.16.0b", [X]) end, 
		    binary_to_list(Binary))).



num_partitions_in_ring() ->
    
    {ok, State} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring:num_partitions(State).

reg_event_manager(Topic) ->
    {ok, Pid} = gen_event:start_link(),
    gproc:reg({n, l, Topic}, Pid).

lookup_event_manager(Topic)  ->
    case gproc:lookup_local_name(Topic) of 
	undefined->
	    undefined;
	Pid -> 
	    gproc:get_value({n,l, Topic}, Pid)
    end.


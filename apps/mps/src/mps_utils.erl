-module(mps_utils).

-export([generate_keys_for_publish/1]).
-export([generate_keys_for_subscribe/1]).
-export([generate_regexpr_for_word/1]).


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


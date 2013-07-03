%%%-------------------------------------------------------------------
%%% File     : kafka_protocol008000000000.erl
%% Kafka Version 0.8   (this file names it 00.80.00)
%% Against
%%   Request            ApiVersion          
%%     ProduceRequest 	     0
%%     FetchRequest 	     0
%%     OffsetRequest 	     0
%%     MetadataRequest 	     0
%%     LeaderAndIsrRequest   0
%%     StopReplicaRequest    0
%%     OffsetCommitRequest   0
%%     OffsetFetchRequest    0 
%%% Author   : Milind Parikh <milindparikh@gmail.com>
%%%-------------------------------------------------------------------
 
-module(kafka_protocol). 
-author('Milind Parikh <milindparikh@gmail.com>').


%% Initial philosophy is derived from  
%%     https://github.com/wooga/kafka-erlang.git
%% The kafka protocol is tested against kafka 0.8 (Beta)



-include("erlkafka.hrl").
-export([create_message_set/1]).

-export ([metadata_request/3, metadata_response/1, produce_request/3, produce_response/1, fetch_request/3, fetch_response/1, offset_request/3, offset_response/1]).
-export ([parse_response_stream/1]).

-record(broker, {node_id, host, port}).
-record(topic_meta_data, {topic_name, partition_meta_data_list}).
-record(partition, {partition_error_code, partition_id, leader_id, replicas, isr}).



%%%-------------------------------------------------------------------
%%%                         API FUNCTIONS
%%%-------------------------------------------------------------------

  

%%%-------------------------------------------------------------------
%%%                         API/METADATA FUNCTION
%%%-------------------------------------------------------------------

%%  @doc The default metadata request
%%           metadata_request(2100000000, "ERLKAFKA", ["testtopic1", "testtopic2"])
%%       


%% -spec metadata_request() -> binary().

-spec metadata_request(
		       CorrelationId::integer(),
                       ClientId::list(),
                       Topics::list()) ->
			      binary().


metadata_request(      CorrelationId,
                       ClientId,
                       Topics) ->





    CommonPartofRequest = common_part_of_request(?RQ_METADATA, 
						  ?API_V_METADATA,
						  CorrelationId,
						  ClientId),
%    io:format("COMMON PART ~p~n", [CommonPartofRequest]),

    TopicCount = length(Topics),

    VarMetaDataRequest = 
	case TopicCount of
 	    0 ->
		<<TopicCount:32/integer, -1:16/integer>>;
	    _Else -> 
		TopicsAsKafkaStrings = 
		    lists:foldl(fun(X, A) -> 

					LOB = size(X), 
					<<LOB:16/integer,X/binary, A/binary>> 
				end, 
				<<"">>, 
				Topics),
		<< TopicCount:32/integer, TopicsAsKafkaStrings/binary>>
	end,
    
    MetaDataRequest = << CommonPartofRequest/binary, VarMetaDataRequest/binary>>,
    SizeMetaDataRequest = size(MetaDataRequest),
    
    <<SizeMetaDataRequest:32/integer, MetaDataRequest/binary>>.



%-spec metadata_response(
%		       Data::binary()) ->
%			      {BrokerList, TopicMetaDataList}
    
metadata_response(Data) ->

    {BrokerList, TopicMetaDataBin} = parse_brokers_info(Data),
    {TopicMetaDataList, _} = parse_topic_meta_data(TopicMetaDataBin),
    {BrokerList, TopicMetaDataList}.





%%%-------------------------------------------------------------------
%%%                         API/PRODUCE FUNCTION
%%%-------------------------------------------------------------------

%%  @doc The         PRODUCE  request
%%           produce_request(0000000001, "ERLKAFKA", [
%%                                                    {"testtopic1", 
%%                                                           [
%%							      {0, [{"MSG1"}, {"MSG2"}]},
%%							      {1, [{"MSG3"}, {"MSG4"}]}
%%                                                           ]
%%						      },       
%%                                                    {"testtopic2", 
%%                                                           [
%%							      {0, [{"MSG5"}, {"com.amazon", "MSG6"}]},
%%							      {1, [{"MSG7"}, {"MSG8"}]}
%%                                                           ]
%%						      }
%%						     ]
%%			     )

                                                      

% -spec        produce_request(
%		       CorrelationId::integer(),
%                       ClientId::list(),
%                       Topics::list()) ->
%			      binary().



produce_request ( CorrelationId,
		  ClientId,
		  Topics
		) ->

    produce_request ( CorrelationId,
		      ClientId,
		      Topics, 
		      ?PRODUCE_RQ_REQUIRED_ACKS,
		      ?PRODUCE_RQ_TIMEOUT).
	
%-spec produce_request(
%	CorrelationId::integer(),
%	ClientId::list(),
%	Topics::list(), 
%	RequiredAcks:integer(),
%	Timeout:integer()
%       ) ->
%			     binary().


produce_request ( CorrelationId,
		  ClientId,
		  Topics, 
		  RequiredAcks,
		  TimeOut
		) ->
    
    CommonPartOfRequest = common_part_of_request(?RQ_PRODUCE, 
						  ?API_V_PRODUCE,
						  CorrelationId,
						  ClientId),

    VarProduceRequest = var_part_of_produce_request(Topics),
	
    ProduceRequest = 
	<<CommonPartOfRequest/binary, 
	  RequiredAcks:16/integer, 
	  TimeOut:32/integer,
	  VarProduceRequest/binary>>,

    ProduceRequestSize = size(ProduceRequest),
    
    <<ProduceRequestSize:32/integer, ProduceRequest/binary>>.

	
			


%-spec produce_response(
%		       Data::binary()) ->
%			      {TopicList}


produce_response(Data) ->
    Topics = parse_topics_for_produce_request(Data),
    {Topics}.





%%%-------------------------------------------------------------------
%%%             Fetch Request             
%%%-------------------------------------------------------------------
%% @doc Generate fetch request - TopicList has list of topics
%%      where each element is tuple containing topic name and 
%%      list of partition along with first off set to be read, sample 
%%      TopicList = [{"topic1", [{0, 0}, {1,0}, {2,0}]}, 
%%				 {"topic2", [{0, 0}, {1,0}, {2,0}]}].

-spec fetch_request(CorrelationId::integer(), ClientId::list(), TopicList::list()) -> binary. 

fetch_request(CorrelationId, ClientId, TopicList) ->
	fetch_request(CorrelationId, ClientId, ?FETCH_RQ_MAX_WAIT_TIME, ?FETCH_RQ_MIN_BYTES, TopicList).

-spec fetch_request(CorrelationId::integer(), ClientId::list(), MaxWaitTime::integer(), MinBytes::integer(), TopicList::list()) -> binary.

fetch_request(CorrelationId, ClientId, MaxWaitTime, MinBytes, TopicList) ->
	%% @TODO: Make partitions optional
	FetchTopicList = lists:foldl(fun(X, List) -> 
              {TopicName, PartitionList} = X,
              Partitions = lists:foldl(fun (Y, PartitionList) ->
								  {Partition, Offset} = Y,
								  [<<Partition:32/integer, Offset:64/integer, ?MAX_BYTES_FETCH:32/integer>> | PartitionList]
								  end, [], PartitionList),
			  PartitionBin = create_array_binary(Partitions),
			  TopicNameBin = create_string_binary(TopicName),
			  [<<TopicNameBin/binary, PartitionBin/binary>> | List]
 			 end, [], TopicList),
	FetchTopicBin = create_array_binary(FetchTopicList),
	CommonPartofRequest = common_part_of_request(?RQ_FETCH, ?API_V_FETCH, CorrelationId, ClientId),
	FetchRequest = <<CommonPartofRequest/binary, ?REPLICA_ID:32/signed-integer, MaxWaitTime:32/integer, MinBytes:32/integer, FetchTopicBin/binary>>,
        FetchRequestSize = size(FetchRequest),
	<<FetchRequestSize:32/integer, FetchRequest/binary>>.


%%%-------------------------------------------------------------------
%%%             Fetch Response             
%%%-------------------------------------------------------------------

-spec fetch_response(Response::binary()) -> tuple().

fetch_response(<<NumberOfTopics:32/integer, Rest/binary >>) -> 
	  parse_fetch_topic_details(NumberOfTopics, Rest, []).











%%%-------------------------------------------------------------------
%%%                         API/OFFSET FUNCTION
%%%-------------------------------------------------------------------

%%  @doc The         OFFSET request
%%           offset_request(0000000001, "ERLKAFKA", [
%%                                                    {<<"testtopic1">>, 
%%                                                           [
%%							      {0, 0, 20000},
%%							      {1, 0, 20000}
%%                                                           ]
%%						      },       
%%                                                    {<<"testtopic2">>,
%%                                                           {
%%							      [0, 0, 20000],
%%							      [1, 0, 20000]
%%                                                           }
%%						      }
%%						     ]
%%			     )

                                                      
%%
%% -spec        offset_request(
%%		       CorrelationId::integer(),
%%                       ClientId::list(),
%%                       Topics::list()) ->
%%			      binary().
%%


offset_request ( CorrelationId,
		  ClientId,
		  Topics
		) ->

    CommonPartOfRequest = common_part_of_request(?RQ_OFFSET, 
						 ?API_V_OFFSET,
						 CorrelationId,
						 ClientId),

    VarOffsetRequest = var_part_of_offset_request(Topics),
	
    OffsetRequest = 
	<<CommonPartOfRequest/binary, 
	  ?REPLICA_ID:32/integer,
	  VarOffsetRequest/binary>>,

    OffsetRequestSize = size(OffsetRequest),
    
    <<OffsetRequestSize:32/integer, OffsetRequest/binary>>.


%-spec offset_response(
%		       Data::binary()) ->
%			      {TopicList}


offset_response(Data) ->
    Topics = parse_topics_for_offset_request(Data),
    {Topics}.





%%%-------------------------------------------------------------------
%%%                       SOCKET DATA PROCESSING FUNCTION 
%%%-------------------------------------------------------------------

%% parse_response_stream should return length consumed & a list of  correlationids and the specific response parsed
%% WHERE POSSIBLE
%% 
parse_response_stream(Data) -> 
%    io:format("Parse this data stream ~p~n", [Data]),
     parse_response_stream(Data, 0, []).

parse_response_stream(<<>>, LengthConsumed, ListOfResponses)  -> 
    {LengthConsumed, ListOfResponses };
      
parse_response_stream (<<ResponseSize:32/integer, CorrId:32/integer, Rest/binary>> = _Data, LengthConsumed, ListOfResponses) when size(Rest) >= ResponseSize - 4 ->
%    io:format("Correlation Id ~w~n", [CorrId]),
    {RequestType, ResponseFun} = get_response_type_n_fun_from_corrid(CorrId),

    ResponseSizeWOCI = ResponseSize - 4,

    <<ResponseData:ResponseSizeWOCI/binary, RestOfRest/binary>> = Rest,
    ResponseFunReturn = ResponseFun(ResponseData),
    parse_response_stream (RestOfRest, LengthConsumed + 4 + ResponseSize, [ {CorrId, RequestType, ResponseFunReturn} |  ListOfResponses]);
    
    
parse_response_stream (_Data, LengthConsumed, ListOfResponses) -> 	    

    {LengthConsumed, ListOfResponses }.


get_response_type_n_fun_from_corrid(CorrId) when  ((CorrId >= ?STARTMD) and (CorrId =< ?ENDMD)) ->
    {?RQ_METADATA, fun metadata_response/1};

get_response_type_n_fun_from_corrid(CorrId) when  ((CorrId >= ?STARTPR) and (CorrId =< ?ENDPR)) ->
    {?RQ_PRODUCE, fun produce_response/1};

get_response_type_n_fun_from_corrid(CorrId) when  ((CorrId >= ?STARTFE) and (CorrId =< ?ENDFE)) ->
    {?RQ_FETCH, fun fetch_response/1};

get_response_type_n_fun_from_corrid(CorrId) when  CorrId == 0 ->
    {?RQ_OFFSET, fun offset_response/1}.






%%%-------------------------------------------------------------------
%%%                         INTERNAL  FUNCTIONS
%%%-------------------------------------------------------------------

common_part_of_request(RequestType, ApiVersion, CorrelationId, ClientId) ->
    
    ClientIdBinary = list_to_binary(ClientId),
    ClientIdSize = size(ClientIdBinary),
    <<RequestType:16/integer, ApiVersion:16/integer, CorrelationId:32/integer, ClientIdSize:16/integer, ClientIdBinary/binary>>.








%%%-------------------------------------------------------------------
%%%                         INTERNAL METADATA FUNCTIONS
%%%-------------------------------------------------------------------


parse_brokers_info(<<BrokerSize:32/integer, Bin/binary>>) ->
    parse_broker_info(BrokerSize, Bin, []).

parse_broker_info(0, Bin, BrokerList) ->  
    {BrokerList, Bin};
parse_broker_info(BrokerSize, Bin, BrokerList) ->
    {Broker, RemainningBin} = parse_broker_info_details(Bin),      
    parse_broker_info((BrokerSize -1), RemainningBin, [Broker | BrokerList]).
        
parse_broker_info_details(<<NodeID:32/integer, HostSize:16/integer, Host:HostSize/binary, Port:32/integer,  Bin/binary>>) ->
						   {#broker{node_id=NodeID, host=Host, port=Port}, Bin}.



parse_topic_meta_data(<<TopicMetaDataSize:32/integer, Bin/binary>>)
             -> parse_topic_meta_data(TopicMetaDataSize, Bin, []).

parse_topic_meta_data(0, Bin, TopicMetaDataList) ->  
                {TopicMetaDataList, Bin};
parse_topic_meta_data(TopicMetaDataSize, Bin, TopicMetaDataList) ->  
    {TopicMetaData, RemainningBin} = parse_topic_meta_data_details(Bin),
    parse_topic_meta_data((TopicMetaDataSize - 1), RemainningBin, [TopicMetaData | TopicMetaDataList]).



parse_topic_meta_data_details(<<TopicSize:32/integer, TopicName:TopicSize/binary, Bin/binary>>) 
           -> {PartitionList, RemainningBin} = parse_partition_meta_data(Bin),
              {#topic_meta_data{topic_name = TopicName, partition_meta_data_list = PartitionList}, RemainningBin}.


parse_partition_meta_data(<<PartitionSize:32/integer, Bin/binary>>) ->
	        parse_partition_meta_data(PartitionSize, Bin, []).

parse_partition_meta_data(0, Bin, PartitionList) ->
    {PartitionList, Bin};
parse_partition_meta_data(PartitionSize, Bin, PartitionList) ->
    {Partition, RemainningBin} = parse_partition_meta_data_details(Bin),
    parse_partition_meta_data((PartitionSize - 1), RemainningBin, [Partition | PartitionList]).

parse_partition_meta_data_details(<<PartitionErrorCode:16/integer, PartitionID:32/integer, LeaderID:32/integer, ReplicaSize:32/integer, Bin/binary>>) ->
            {ReplicaList, <<IsrSize:32/integer, IsrBin/binary>>} = retrieve_replica(ReplicaSize, Bin, [] ),
			{IsrList, RemainningBin} = retrieve_isr(IsrSize, IsrBin, []),
			{#partition{partition_error_code = PartitionErrorCode, partition_id = PartitionID, leader_id = LeaderID, replicas = ReplicaList, isr = IsrList}, RemainningBin}. 
	



retrieve_replica(0, Bin, ReplicaList) ->
    {ReplicaList, Bin};

retrieve_replica(ReplicaSize, Bin, ReplicaList) ->
    {Replica, RemainningBin} = retrieve_replica(Bin),      
    retrieve_replica((ReplicaSize -1), RemainningBin, [Replica | ReplicaList]).
                 
retrieve_replica(<<Replica:32/integer, Bin/binary>>) -> {Replica, Bin}.	




retrieve_isr(0, Bin, IsrList) -> 
    {IsrList, Bin};
retrieve_isr(IsrSize, Bin, IsrList) ->
    {Isr, RemainningBin} = retrieve_isr_details(Bin),      
    retrieve_isr((IsrSize -1), RemainningBin, [Isr | IsrList]).
             
retrieve_isr_details(<<Isr:32/integer, Bin/binary>>) -> {Isr, Bin}.	    



%%%-------------------------------------------------------------------
%%%                         INTERNAL PRODUCE REQUEST FUNCTIONS
%%%-------------------------------------------------------------------


var_part_of_produce_request (Topics) ->

    TopicCount = length(Topics),
    
    TopicRequests = 
	lists:foldl(fun (X, A1) ->
			    {TopicName, PartitionInfo} = X, 
			    
			    TopicNameSize = size(TopicName),
			    PartitionSize = length(PartitionInfo),
			    
			    VarP = 
				lists:foldl(fun(Y, A2) ->
						    {PartitionId, ListMessages} = Y,
						    
						    MessageSet = create_message_set(ListMessages),
						    MessageSetSize = size(MessageSet),
						    
						    <<PartitionId:32/integer, MessageSetSize:32/integer, MessageSet/binary, A2/binary>>
					    end,
					    <<"">>,
					    PartitionInfo),
			    <<TopicNameSize:16/integer, TopicName/binary, PartitionSize:32/integer, VarP/binary, A1/binary>>
		    end,
		    <<"">>,
		    Topics),
    <<TopicCount:32/integer, TopicRequests/binary>>.





parse_topics_for_produce_request (<<NumberOfTopics:32/integer, Bin/binary>>) ->
    parse_topics_for_produce_request(NumberOfTopics, Bin, []).

parse_topics_for_produce_request(0, _Bin, Topics) ->
    Topics;
parse_topics_for_produce_request(RemainingCount, <<TopicLength:16/integer, TopicName:TopicLength/binary, PartitionLength:32/integer, RemainingBin/binary>>, Topics ) ->
    {ListPartitions, RestOfRemainingBin} = parse_partitions_for_produce_request (PartitionLength, RemainingBin),
    parse_topics_for_produce_request(RemainingCount-1, RestOfRemainingBin, [{TopicName, [ListPartitions]} | Topics]).

parse_partitions_for_produce_request(PartitionLength, RemainingBin) ->
    parse_partitions_for_produce_request(PartitionLength, RemainingBin, []).
    
parse_partitions_for_produce_request(0, Bin, ListPartitions) ->
    {ListPartitions, Bin};


parse_partitions_for_produce_request(RemainingPartitions, <<PartitionId:32/integer, ErrorCode:16/integer, Offset:64/integer, RestOfBin/binary>>, ListPartitions) ->
    parse_partitions_for_produce_request(RemainingPartitions-1, RestOfBin, [{PartitionId, ErrorCode, Offset} | ListPartitions]).



create_message_set (ListMessages) ->
    RListMessages = lists:reverse(ListMessages),
    MessageSet = 
	lists:foldl(fun (X, A) ->
			    Message=create_message(X),
%			    io:format("~p~n", [Message]),
			    MessageSize = size(Message),

			    <<?PRODUCE_OFFSET_MESSAGE:64/integer, MessageSize:32/integer,Message/binary, A/binary>>
		    end,
		    <<"">>,
		    RListMessages),
    <<MessageSet/binary>>.


create_message({}) ->	
    create_message({<<"">>, <<"">>});

create_message({Value}) ->	
    create_message({<<"">>, Value});

create_message({Key, Value}) ->	
    create_message({Key, Value, ?COMPRESSION_NONE, ?MAGIC_BYTE});

create_message({Key, Value, Compression}) ->	
    create_message({Key, Value, Compression, ?MAGIC_BYTE});



create_message({Key, Value, Compression, Version}) ->
    KeyValueBin = create_key_value_bin(Key, Value),
    MessageBin = <<Version:8/integer, Compression:8/integer, KeyValueBin/binary>>,
    Crc32 = erlang:crc32(MessageBin),
    Message = <<Crc32:32/integer, MessageBin/binary>>,
    
%    io:format("~p ~p~n", [MessageBin, Crc32]),
%    io:format("~p~n", [Message]),
    Message.

    
create_key_value_bin(<<"">>, <<"">>) ->
	<<-1:32/big-signed-integer, -1:32/signed-integer>>;    
    
create_key_value_bin(<<"">>, Value) ->
	ValueBin = Value,
	ValueSize = size(ValueBin),
	<<-1:32/big-signed-integer, ValueSize:32/big-signed-integer, ValueBin/binary>>;
    
create_key_value_bin(Key, Value) -> 
	ValueBin = Value,
	ValueSize = size(ValueBin) ,
	KeyBin = Key,
	KeySize = size(KeyBin),
	<<KeySize:32/integer, KeyBin/binary, ValueSize:32/integer, ValueBin/binary>>.









%%%-------------------------------------------------------------------
%%%                         INTERNAL FETCH REQUEST FUNCTIONS
%%%-------------------------------------------------------------------



var_part_of_fetch_request (Topics) ->

    TopicCount = length(Topics),
    
    TopicRequests = 
	lists:foldl(fun (X, A1) ->
			    {TopicName, PartitionInfo} = X, 
%			    io:format("PARTITION INFO ~p~n", [PartitionInfo]),
%			    io:format("Topic Name = ~p~n", [TopicName]),
			    TopicNameSize = size(TopicName),
			    PartitionSize = length(PartitionInfo),
%			    io:format("Partition Size = ~p~n", [PartitionSize]),
			    VarP = 
				lists:foldl(fun(Y, A2) ->
						    {PartitionId, FetchOffset,MaxBytes } = Y,
						    <<PartitionId:32/integer, FetchOffset:64/integer, MaxBytes:32/integer, A2/binary>>
					    end,
					    <<"">>,
					    PartitionInfo),
%			    io:format("VAR INFO = ~p~n", [VarP]),
%			    SendMsg = <<TopicNameSize:16/integer,
%			      TopicName/binary,
%			      PartitionSize:32/integer, VarP/binary, A1/binary>>,
			    
%			    io:format("SEND MSG = ~p~n", [SendMsg]),
			    <<TopicNameSize:16/integer,
			      TopicName/binary,
			      PartitionSize:32/integer, VarP/binary, A1/binary>>
		    end,
		    <<"">>,
		    Topics),
    <<TopicCount:32/integer, TopicRequests/binary>>.


parse_topics_for_fetch_request (<<NumberOfTopics:32/integer, Bin/binary>>) ->
%    io:format("Number of Topics   = ~p  and Bin = ~p~n", [NumberOfTopics, Bin]),
    parse_topics_for_fetch_request(NumberOfTopics, Bin, []).

parse_topics_for_fetch_request(0, _Bin, Topics) ->
    Topics;

parse_topics_for_fetch_request(RemainingCount, <<TopicLength:16/integer, TopicName:TopicLength/binary, PartitionLength:32/integer, RemainingBin/binary>>, Topics ) ->
%    io:format("Topic Name = ~p~n", [TopicName]),
%    io:format("Partition Length = ~p~n", [PartitionLength]),
    
    {ListPartitions, RestOfRemainingBin} = parse_partitions_for_fetch_request (PartitionLength, RemainingBin),
    parse_topics_for_fetch_request(RemainingCount-1, RestOfRemainingBin, [{TopicName, [ListPartitions]} | Topics]);

parse_topics_for_fetch_request(_, _Bin, Topics) ->
    Topics.


parse_partitions_for_fetch_request(PartitionLength, RemainingBin) ->
    parse_partitions_for_fetch_request(PartitionLength, RemainingBin, []).


parse_partitions_for_fetch_request(0, Bin, ListPartitions) ->
    {ListPartitions, Bin};


parse_partitions_for_fetch_request(RemainingPartitions, 
				   <<PartitionId:32/integer,
				     ErrorCode:16/integer, 
				     HighWaterMarkOffset:64/integer, 
				     0:32/integer, 
				     RestOfBin/binary>>, 
				   ListPartitions) ->

    parse_partitions_for_fetch_request(RemainingPartitions-1, RestOfBin, [{PartitionId, ErrorCode, HighWaterMarkOffset, <<"">>} | ListPartitions]);




parse_partitions_for_fetch_request(RemainingPartitions, 
				   <<PartitionId:32/integer,
				     0:16/integer, 
				     HighWaterMarkOffset:64/integer, 
				     MessageSetSize:32/integer, 
				     MessageSet:MessageSetSize/binary,
				     RestOfBin/binary>>, 
				   ListPartitions) ->
    
    parse_partitions_for_fetch_request(RemainingPartitions-1, RestOfBin, [{PartitionId, 0, HighWaterMarkOffset, parse_message_set_for_fetch_request (MessageSet)} | ListPartitions]);


parse_partitions_for_fetch_request(RemainingPartitions, 
				   <<PartitionId:32/integer,
				     ErrorCode:16/integer, 
				     HighWaterMarkOffset:64/integer, 
				     MessageSetSize:32/integer, 
				     _MessageSet:MessageSetSize/binary,
				     RestOfBin/binary>>, 
				   ListPartitions) ->
    
    parse_partitions_for_fetch_request(RemainingPartitions-1, RestOfBin, [{PartitionId, ErrorCode, HighWaterMarkOffset, <<"">>} | ListPartitions]);




parse_partitions_for_fetch_request(_, _Bin, ListPartitions) ->
    {ListPartitions, <<"">>}.


parse_message_set_for_fetch_request (MessageSet) ->
%    io:format("MessageSet = ~p~n", [MessageSet]),
    parse_message_set_for_fetch_request (MessageSet, []).

parse_message_set_for_fetch_request (<<"">>, ListMessages)->
    ListMessages;

parse_message_set_for_fetch_request (<<Offset:64/integer, MessageSize:32/integer, RestOfMessage:MessageSize/binary, RestOfMessageSet/binary>>, ListMessages)->
%    io:format("ListMessages, RestOfMessage = ~p ~p~n", [ListMessages, RestOfMessage]),
    parse_message_set_for_fetch_request (RestOfMessageSet,  [parse_message_for_fetch_request(Offset, RestOfMessage) | ListMessages ]);

parse_message_set_for_fetch_request (_, ListMessages)->
    ListMessages.

parse_message_for_fetch_request(Offset, <<Crc:32/integer, RestOfMessage/binary>>)->
    case erlang:crc32(RestOfMessage) == Crc of
	false ->
	    {corrupted_msg};
	true  ->
%	    io:format("RestOfMessage = ~p~n", [RestOfMessage]),
	    parse_crcd_message_for_fetch_request(Offset, RestOfMessage)
    end.




parse_crcd_message_for_fetch_request(Offset, <<MagicBytes:8/integer, Attributes:8/integer, -1:32/signed-integer,  -1:32/signed-integer>> ) ->
    {Offset, MagicBytes, Attributes, <<"">>, <<"">>};

parse_crcd_message_for_fetch_request(Offset, <<MagicBytes:8/integer, Attributes:8/integer, -1:32/signed-integer, ValueSize:32/signed-integer, Value:ValueSize/binary>> ) ->
    {Offset, MagicBytes, Attributes, <<"">>, Value};


parse_crcd_message_for_fetch_request(Offset, <<MagicBytes:8/integer, Attributes:8/integer, KeySize:32/signed-integer, Key:KeySize/binary, -1:32/signed-integer>> ) ->
    {Offset, MagicBytes, Attributes, Key, <<"">>};


parse_crcd_message_for_fetch_request(Offset, <<MagicBytes:8/integer, Attributes:8/integer, KeySize:32/signed-integer, Key:KeySize/binary, ValueSize:32/signed-integer, Value:ValueSize/binary>> ) ->
    {Offset, MagicBytes, Attributes, Key, Value};


parse_crcd_message_for_fetch_request(_Offset, _ ) ->
    {}.


							 
    


%%%-------------------------------------------------------------------
%%%                         INTERNAL OFFSET REQUEST FUNCTIONS
%%%-------------------------------------------------------------------


var_part_of_offset_request (Topics) ->

    TopicCount = length(Topics),
    
    TopicRequests = 
	lists:foldl(fun (X, A1) ->
			    {TopicName, PartitionInfo} = X, 

			    
			    TopicNameSize = size(TopicName),
			    PartitionSize = length(PartitionInfo),
			    
			    VarP = 
				lists:foldl(fun(Y, A2) ->
						    {PartitionId, Time, MaxNumberOfOffsets } = 
							var_part_of_partition_offset_request(Y),
						    <<PartitionId:32/integer, Time:64/integer, MaxNumberOfOffsets:32/integer, A2/binary>>
					    end,
					    <<"">>,
					    PartitionInfo),
			    <<TopicNameSize:16/integer, TopicName/binary, PartitionSize:32/integer, VarP/binary, A1/binary>>
		    end,
		    <<"">>,
		    Topics),
    <<TopicCount:32/integer, TopicRequests/binary>>.



var_part_of_partition_offset_request ({PartitionId}) ->
    {PartitionId, ?LATEST_OFFSET, 1};
var_part_of_partition_offset_request ({PartitionId, Time}) ->
    {PartitionId, Time, 1};
var_part_of_partition_offset_request ({PartitionId, Time, MaxNumberOfOffsets}) ->
    {PartitionId, Time, MaxNumberOfOffsets }.




parse_topics_for_offset_request (<<NumberOfTopics:32/integer, Bin/binary>>) ->
    parse_topics_for_offset_request(NumberOfTopics, Bin, []).

parse_topics_for_offset_request(0, _Bin, Topics) ->
    Topics;

parse_topics_for_offset_request(RemainingCount, <<TopicLength:16/integer, TopicName:TopicLength/binary, PartitionLength:32/integer, RemainingBin/binary>>, Topics ) ->
    {ListPartitions, RestOfRemainingBin} = parse_partitionoffsets_for_offset_request (PartitionLength, RemainingBin),
    parse_topics_for_offset_request(RemainingCount-1, RestOfRemainingBin, [{TopicName, ListPartitions} | Topics]).


parse_partitionoffsets_for_offset_request(PartitionLength, RemainingBin) ->
    parse_partitionoffsets_for_offset_request(PartitionLength, RemainingBin, []).


parse_partitionoffsets_for_offset_request(0, Bin, ListPartitions) ->
    {ListPartitions, Bin};


parse_partitionoffsets_for_offset_request(RemainingPartitions, 
				   <<PartitionId:32/integer,
				     ErrorCode:16/integer, 
				     OffsetsLength:32,
				     RestOfBin/binary>>, 
				   ListPartitions) ->
    {ListOffsets, RestOfRemainingBin} = parse_offsets_for_offset_requests(OffsetsLength, RestOfBin, []),
    parse_partitionoffsets_for_offset_request(RemainingPartitions-1, RestOfRemainingBin, [{PartitionId, ErrorCode, ListOffsets} | ListPartitions]).

parse_offsets_for_offset_requests(0, RestOfBin, ListOffsets) ->
    {ListOffsets, RestOfBin};
parse_offsets_for_offset_requests(RemainingOffsets, <<Offset:64/integer, RestOfBin/binary>>, ListOffsets) ->
    parse_offsets_for_offset_requests(RemainingOffsets -1, RestOfBin, [Offset | ListOffsets]).

%% Parsed payload response

-spec parse_fetch_topic_details(NumberOfTopics::integer(), TopicBin::binary(), Data::list()) -> tuple.

parse_fetch_topic_details(0, TopicBin, Data) -> 
	Data;
parse_fetch_topic_details(NumberOfTopics, TopicBin, Data) -> 
	%% Parse Topic Details
    <<TopicNameSize:16/integer, TopicName:TopicNameSize/binary, NumberOfPartitions:32/integer, PartitionBin/binary>> = TopicBin,
	{RemainningBin, PartitionList} = parse_fetch_partition_details(NumberOfPartitions, PartitionBin, []),
	parse_fetch_topic_details((NumberOfTopics - 1), RemainningBin, [{TopicName, PartitionList} | Data ]).

-spec parse_fetch_partition_details(NumberOfPartitions::integer(), PartitionBin::binary(), Data::list()) -> tuple.

parse_fetch_partition_details(0, PartitionBin, Data) ->
	{PartitionBin, Data};
parse_fetch_partition_details(NumberOfPartitions, PartitionBin, Data) ->
	<<Partition:32/integer, ErrorCode:16/integer, HighwaterMarkOffset:64/integer, MessageSetSize:32/integer, Bin/binary>> = PartitionBin,
	<<MessageBin:MessageSetSize/binary, RemainningBin/binary>> = Bin,
    MessageList = parse_fetch_message_details(MessageBin, []),
    parse_fetch_partition_details((NumberOfPartitions- 1), RemainningBin, [{Partition, MessageList} | Data]). 

-spec parse_fetch_message_details(MessageBin::binary(), Data::list()) -> list.

parse_fetch_message_details(<<>>, Data) -> Data;
parse_fetch_message_details(MessageBin, Data) -> 
	<<OffSet:64/integer, MessageSize:32/integer,CRC32:32/integer, MagicByte:8/integer, Attributes:8/integer, KeySize:32/signed-integer, KeyBin/binary>> = MessageBin,
	{Key, ValueBin} = parse_bytes(KeySize, KeyBin),
	<<ValueSize:32/signed-integer, Values/binary>> = ValueBin,
	{Value, RemainningBin} = parse_bytes(ValueSize, Values),
	parse_fetch_message_details(RemainningBin, [{OffSet, Key, Value} | Data]).

parse_bytes(-1, Bin) ->
	{<<>>, Bin};
parse_bytes(BytesSize, Bin) ->
	<<Bytes:BytesSize/binary, RemainningBin/binary>> = Bin,
	{Bytes, RemainningBin}.

parse_integer_array(-1, SizeOfInteger, DataList, Bin) ->
	{DataList, Bin};
parse_integer_array(0, SizeOfInteger, DataList, Bin) ->
	{DataList, Bin};
parse_integer_array(ListSize, SizeOfInteger, DataList, Bin) ->
	<<Value:SizeOfInteger/integer, RemainningBin/binary>> = Bin,
	parse_integer_array((ListSize - 1), SizeOfInteger, [Value | DataList], RemainningBin).

%% @doc Create binary value as per kafka protocol from string.
-spec create_string_binary(StringValue::list()) -> binary.
create_string_binary([]) ->
	<<-1:16/signed-integer>>;
create_string_binary(StringValue) ->
	StringBin = list_to_binary(StringValue),
	StringSize = size(StringBin),
	<<StringSize:16/integer, StringBin/binary>>.

%% @doc Create binary value as per kafka protocol from list.
-spec create_array_binary(ListValues::list()) -> binary.
create_array_binary([]) ->
	<<-1:32/signed-integer>>;
create_array_binary(ListValues) ->
	NumberOfValues = length(ListValues),
	ValueBin = list_to_binary(ListValues),
	<<NumberOfValues:32/integer, ValueBin/binary>>.

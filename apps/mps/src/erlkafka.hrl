%%%-------------------------------------------------------------------
%%% File     : erlkafka.hrl
%%% Author   : Milind Parikh <milindparikh@gmail.com>
%%%-------------------------------------------------------------------
-author("Milind Parikh <milindparikh@gmail.com> [http://www.milindparikh.com]").

-define(MAX_MSG_SIZE, 1048576).
-define(REPLICA_ID, -1).

-define(PRODUCE_OFFSET_MESSAGE, 0).

-define(PRODUCE_RQ_REQUIRED_ACKS, 1).
-define(PRODUCE_RQ_TIMEOUT, 1000).
-define(FETCH_RQ_MAX_WAIT_TIME, 1).
-define(FETCH_RQ_MIN_BYTES, 0).

-define(EARLIEST_OFFSET, -2).
-define(LATEST_OFFSET, -1).
-define(MAX_OFFSETS, 20).

-define(RQ_PRODUCE, 0).
-define(RQ_FETCH, 1).
-define(RQ_OFFSET, 2).
-define(RQ_METADATA, 3).
-define(RQ_LEADERANDISR, 4).
-define(RQ_STOPREPLICA, 5).
-define(RQ_OFFSETCOMMIT, 6).
-define(RQ_OFFSETFETCH, 7).

-define(API_V_PRODUCE, 0). 
-define(API_V_FETCH, 0).
-define(API_V_OFFSET, 0).
-define(API_V_METADATA, 0).
-define(API_V_LEADERANDISR, 0).
-define(API_V_STOPREPLICA, 0).
-define(API_V_OFFSETCOMMIT, 0).
-define(API_V_OFFSETFETCH, 0).


-define(COMPRESSION_NONE, 0).
-define(COMPRESSION_GZIP, 1).
-define(COMPRESSION_SNAPPY, 2).

-define(MAGIC_BYTE, 0).
-define(ERR_NONE, 0).
-define(ERR_UNKNOWN, -1).
-define(ERR_OFFSETOUTOFRANGE, 1).
-define(ERR_INVALIDMESSAGE, 2).
-define(ERR_UNKNOWNTOPICORPARTITION, 3).
-define(ERR_INVALIDMESSAGESIZE,4).
-define(ERR_LEADERNOTAVAILABLE, 5).
-define(ERR_NOTLEADERFORPARTITION, 6).
-define(ERR_REQUESTTIMEDOUT, 7).
-define(ERR_BROKERNOTAVAILABLE, 8).
-define(ERR_REPLICANOTAVAILABLE, 9).
-define(ERR_MESSAGESIZETOOLARGE, 10).
-define(ERR_STALECONTROLLEREPOCHCODE, 11).
-define(ERR_OFFSETMETADATATOOLARGECODE, 12).

-define(MAX_BYTES_FETCH, 10000)

-define(STARTMD, 2000000000).
-define(ENDMD,   2999999999).
-define(STARTFE, 1).
-define(ENDFE,    999999999).
-define(STARTPR, 1000000000).
-define(ENDPR,   1999999999).
-define(STARTOR, 3000000000).
-define(ENDOR,   3500000000).
-define(STARTOF, 3500000001).
-define(ENDOF,   3999999999).
-define(STARTOC, 4000000000).
-define(ENDOC,   4294967295).

-module(simple_riak_pool).
-behavior(gen_pool).
-export([
    start_link/1,
    do/2
]).
-export([
    connection/1
]).

start_link(Options) ->
    gen_pool:start_link(?MODULE, ?MODULE, Options).

do(Fun, Extra) ->
    Do = fun (Pid, _) ->
            erlang:apply(riakc_pb_socket, Fun, [Pid | Extra])
        end,
    gen_pool:q(?MODULE, Do, []).

connection(Options) ->
    Host = proplists:get_value(host, Options, "127.0.0.1"),
    Port = proplists:get_value(port, Options, 8087),
    {ok, Pid} = riakc_pb_socket:start_link(Host, Port, [auto_reconnect, {connect_timeout, 1000}]),
    Pid.

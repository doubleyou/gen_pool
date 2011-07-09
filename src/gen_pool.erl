%% @doc Generic pool behavior
-module(gen_pool).

-export([behaviour_info/1]).
-export([
    start_link/3,
    q/3
]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-record(state, {
    module :: atom(),
    connection_options = [] :: list(),
    query_options = [] :: list(),
    pool_size = 10 :: integer(),
    free_pool :: queue(),
    busy_pool = [] :: list()
}).

%%
%% Behavior info
%%

behaviour_info(callbacks) ->
    [
        {connection, 1}
    ];
behaviour_info(_) ->
    undefined.

%%
%% External API exports
%%

%% @doc Starts a generic pool process registered as Name.
%%      This process creates a pool of connections created using
%%      multiple calls of Module:connection/1 and allows automatic
%%      load balancing and restarting of those connections.
%%
%%      Options is a proplist with possible values:
%%
%%          <p>connection_options - list of options that is passed to
%%              Module:connection/1
%%          </p>
%%          <p>query_options - list of options that is passed to a function
%%              called withing gen_pool:q/3 (see below)
%%          </p>
%%          <p>pool_size – number of connections in the pool
%%          </p>
%%
-spec start_link(Name :: atom(), Module :: atom(), Options :: list()) -> {ok, Pid :: pid()} | ignore | {error, Error :: term()}.
start_link(Name, Module, Options) ->
    gen_server:start_link({local, Name}, ?MODULE, {Module, Options}, []).

%% @doc Picks a free connection from the given generic pool registered as Name
%%      and calls the given function Fun passing this connection pid to it.
%%      The function Fun takes 2 parameters: the connection pid and the proplist
%%      of options which is a combination of Options and the pool query options
%%      that can be passed at gen_pool:start_link/3.
%%      Whether the function Fun fails or not, at the end the picked connection %%      is returned back to the pool as free
%%
-spec q(Name :: atom(), Fun :: fun(), Options :: list()) -> Result :: term().
q(Name, Fun, Options) ->
    {Pid, QueryOptions} = gen_server:call(Name, q),
    try Fun(Pid, Options ++ QueryOptions)
    catch C:E -> erlang:C({E, erlang:get_stacktrace()})
    after gen_server:cast(Name, {free, Pid})
    end.

%%
%% gen_server API exports
%%

init({Module, Options}) ->
    process_flag(trap_exit, true),
    State = parse_options(Options, #state{ module = Module }),
    Pool = queue:from_list(
        [spawn_connection(State) || _ <- lists:seq(1, State#state.pool_size)]
    ),
    {ok, State#state{ free_pool = Pool }}.

handle_call(q, _From, State = #state{ free_pool = {[], []} }) ->
    {reply, all_busy, State};
handle_call(q, _From, State = #state{ free_pool = Free, busy_pool = Busy,
                                        query_options = QueryOptions }) ->
    {{value, Pid}, NewFree} = queue:out(Free),
    NewState = State#state{ free_pool = NewFree, busy_pool = [Pid | Busy] },
    {reply, {Pid, QueryOptions}, NewState}.

handle_cast({free, Pid}, State = #state{ free_pool = Free, busy_pool = Busy}) ->
    NewState = State#state{
        free_pool = queue:in(Pid, Free),
        busy_pool = Busy -- [Pid]
    },
    {noreply, NewState}.

handle_info({'DOWN', _, process, Pid, _}, State = #state{ free_pool = Free, 
                                            busy_pool = Busy }) ->
    {NewFree, NewBusy} = case Busy -- [Pid] of
        Busy -> {queue:filter(fun (P) -> P =/= Pid end, Free), Busy};
        NBusy -> {Free, NBusy}
    end,
    NewState = State#state{
        free_pool = queue:in(spawn_connection(State), NewFree),
        busy_pool = NewBusy
    },
    {noreply, NewState}.

terminate(_Reason, #state{ free_pool = Free, busy_pool = Busy }) ->
    kill_connections(Free),
    P = spawn(fun () ->
        receive
            kill -> kill_connections(Busy)
        end
    end),
    erlang:send_after(?BUSY_CONNECTIONS_KILL_TIMOUT, P, kill),
    ok.

%%
%% Internal API
%%

spawn_connection(#state{ module = Module, connection_options = ConnOpts }) ->
    Pid = Module:connection(ConnOpts),
    erlang:monitor(process, Pid),
    Pid.

kill_connections(Conns) ->
    lists:map(fun (Pid) -> exit(Pid, kill) end, Conns).

parse_options([], State) ->
    State;
parse_options([{pool_size, PoolSize} | Options], State) ->
    parse_options(Options, State#state{ pool_size = PoolSize });
parse_options([{connection_options, ConnectionOpts} | Options], State) ->
    parse_options(Options, State#state{ connection_options = ConnectionOpts });
parse_options([{query_options, QueryOpts} | Options], State) ->
    parse_options(Options, State#state{ query_options = QueryOpts }).

-module(tar_server).

-define(TIMEOUT, 300).

-behavior(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-export([start_link/0, start_link/1, send_document/2, ack/1]).

-record(state, {directory, limit, suffix, extension, messages, timer,
                timeout, ready, workers}).

%% ===================================================================
%% API
%% ===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start_link(Args) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

send_document(Tag, Body) ->
    gen_server:cast(?MODULE, {document, Body, Tag}).

ack(Tags) ->
    gen_server:cast(?MODULE, {ack, Tags}).

%% ===================================================================
%% Callbacks
%% ===================================================================

init(Args) ->
    process_flag(trap_exit, true),
    Dir = proplists:get_value(directory, Args),
    Limit = proplists:get_value(limit, Args),
    Suffix = proplists:get_value(suffix, Args),
    Ext = proplists:get_value(extension, Args),
    [X|_] = Ext,
    Extension = case X of
        $. -> Ext;
        _ -> lists:concat([".", Ext])
    end,
    Timeout = proplists:get_value(timeout, Args, ?TIMEOUT),
    TimeoutMs = case Timeout of
                    0 -> ?TIMEOUT * 1000;
                    _ -> Timeout * 1000
                end,
    {ok, #state{directory = Dir, limit = Limit, suffix = Suffix,
                extension = Extension, messages = [], timer = no_timer,
                timeout = TimeoutMs, ready = [], workers = 0}}.

handle_info({timeout}, State) ->
    if State#state.workers > 0 ->
           Timer = update_timer(no_timer, 1000),
           {noreply, State#state{timer = Timer}};
       State#state.workers =< 0 ->
           case length(State#state.messages) of
               0 -> io:format("~ntimeout waiting for documents"),
                    rmq_consume_server:stop(),
                    {noreply, State#state{timer = no_timer}};
               _ -> spawn_link(fun() -> create_tar(State#state.directory,
                                                   State#state.messages,
                                                   State#state.suffix,
                                                   State#state.extension)
                               end),
                    Workers = State#state.workers + 1,
                    Timer = update_timer(no_timer, 1000),
                    {noreply, State#state{timer = Timer, messages = [],
                                          workers = Workers}}
           end
    end;

handle_info({'EXIT', _Pid, normal}, State) ->
    {noreply, State#state{workers = State#state.workers - 1}};

handle_info(_, State) ->
    {noreply, State}.

handle_call(Message, _From, State) ->
    {stop, Message, State}.

handle_cast({document, Body, Tag}, State) ->
    {Msgs, NewWorker} = maybe_create_tar(State#state.directory,
                                         [{Body, Tag}|State#state.messages],
                                         State#state.suffix,
                                         State#state.extension,
                                         State#state.limit),
    Workers = case NewWorker of
                  true -> State#state.workers + 1;
                  false -> State#state.workers
              end,
    Timer = update_timer(State#state.timer, State#state.timeout),
    {noreply, State#state{messages = Msgs, timer = Timer, workers = Workers,
                          ready = [{Tag, false}|State#state.ready]}};

handle_cast({ack, Acks}, State) ->
    Ready = maybe_send_ack(State#state.ready, Acks),
    {noreply, State#state{ready = Ready}};

handle_cast(_Message, State) ->
    {noreply, State}.

terminate(Reason, _State) ->
    io:format("tar server terminating, reason: ~w~n", [Reason]),
    ok.

code_change(_OldVersion, State, _Extra) ->
    {ok, State}.

%% ===================================================================
%% Private
%% ===================================================================

update_timer(OldTimer, Timeout) ->
    case OldTimer of
        no_timer ->
            erlang:send_after(Timeout, self(), {timeout});
        _ ->
            erlang:cancel_timer(OldTimer),
            erlang:send_after(Timeout, self(), {timeout})
    end.

next_filename(Directory, Suffix, N) ->
    Basename = lists:concat([N, Suffix, ".tar.gz"]),
    FileName = filename:join(Directory, Basename),
    case file:open(FileName, [write, exclusive]) of
        {ok, Device} -> file:close(Device),
                        FileName;
        {error, eexist} -> next_filename(Directory, Suffix, N + 1);
        {error, Reason} -> error("error creating tarfile: " ++ Reason)
    end.

next_filename(Directory, Suffix) ->
    next_filename(Directory, Suffix, 0).

create_tar(Directory, Documents, Suffix, Ext) ->
    Files = [{lists:concat([Id, Ext]), Body} || {Id, {Body, _Tag}}
             <- lists:zip(lists:seq(1, length(Documents)), Documents)],
    TarName = next_filename(Directory, Suffix),
    Start = now(),
    erl_tar:create(TarName, Files, [compressed]),
    _Diff = timer:now_diff(now(), Start) / 1000000.0,
    %io:format("creating ~p took ~.4f seconds~n", [TarName, Diff]),
    ack([Tag || {_Body, Tag} <- Documents]).

maybe_create_tar(Directory, Documents, Suffix, Ext, Limit) ->
    case length(Documents) of
        Limit ->
            spawn_link(fun() -> create_tar(Directory, Documents, Suffix, Ext)
                       end),
            {[], true};
        _ ->
            {Documents, false}
    end.

send_ack(Tag) ->
    rmq_consume_server:ack(Tag, true).

maybe_send_ack([], []) ->
    [];

maybe_send_ack(ReadyList, Acks) ->
    L = lists:map(fun({X, false}) -> {X, lists:member(X, Acks)};
                     ({X, true}) -> {X, true} end, ReadyList),
    Full = lists:sort(L),
    [{First, _}|_] = Full,
    Max = lists:foldl(fun({X, true}, Acc) when X =:= Acc + 1 -> X;
                         (_, Acc) -> Acc end, First - 1, Full),
    if Max =< First -> Full;
       Max > First -> send_ack(Max),
                      lists:dropwhile(fun({X, _}) when X =< Max -> true;
                                         (_) -> false end, Full)
    end.

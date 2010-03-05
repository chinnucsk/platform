%%%----------------------------------------------------------------------
%%% File    : amqp.erl
%%% Author  : Ery Lee <ery.lee@gmail.com>
%%% Purpose : amqp client api
%%% Created : 03 Aug 2009
%%% License : http://www.opengoss.com
%%% Descr   : implement api like ruby amqp library tmm1
%%% 
%%% Copyright (C) 2007-2009, www.opengoss.com 
%%%----------------------------------------------------------------------
-module(amqp).

-include_lib("elog.hrl").

-include("amqp_client.hrl").

-export([start_link/1, stop/0]).

%%api 
-export([queue/0, queue/1, queue/2, 
         exchange/1, exchange/2, exchange/3,
         direct/1, direct/2, 
         topic/1, topic/2, 
         fanout/1, fanout/2, 
         bind/2, bind/3, 
         unbind/2,
         send/2,
         publish/2, publish/3, publish/4,
         delete/2,
         consume/1, consume/2, 
         get/1,
         ack/1, 
         cancel/1 %,close/0
         ]).

%%callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {params, realm, connection, channel, ticket}).

%% @spec start_link(Opts) -> Result
%%  Opts = [tuple()]
%%  Result = {ok, pid()}  | {error, Error}  
%% @doc stop amqp client
start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Opts], []).

%% @spec stop() -> ok
%% @doc stop amqp client
stop() ->
    gen_server:call(?MODULE, stop).

%% @spec queue() -> Result
%%  Name = iolist()
%%  Result = {ok, Q, Props} | {error,Error}
%%  Q = iolist()
%%  Props = [tuple()]
%% @doc declare temporary queue
queue() ->
    call(queue).

%% @spec queue(Name) -> Result
%%  Name = iolist()
%%  Result = ok | {error,Error}
%% @doc declare amqp queue
queue(Name) ->
    call({queue, binary(Name)}).

%% @spec queue(Name, Opts) -> Result
%%  Name = iolist()
%%  Opts = list()
%%  Result = ok | {error,Error}
%% @doc declare amqp queue
queue(Name, Opts) ->
    call({queue, binary(Name), Opts}).

%% @spec exchange(Name) -> Result
%%  Name = iolist()
%%  Result = ok | {error,Error}
%% @doc declare amqp exchange with default type 'direct'
exchange(Name) ->
    call({exchange, binary(Name)}).

%% @spec exchange(Name, Type) -> Result
%%  Name = iolist()
%%  Type = iolist()
%%  Result = ok | {error,Error}
%% @doc declare amqp exchange
exchange(Name, Type) ->
    call({exchange, binary(Name), Type}).

%% @spec exchange(Name, Type, Opts) -> Result
%%  Name = iolist()
%%  Type = iolist()
%%  Opts = list()
%%  Result = ok | {error,Error}
%% @doc declare amqp exchange
exchange(Name, Type, Opts) ->
    call({exchange, binary(Name), Type, Opts}).

%% @spec direct(Name) -> Result
%%  Name = iolist()
%%  Result = ok | {error,Error}
%% @doc declare amqp direct exchange
direct(Name) ->
    call({direct, binary(Name)}).

%% @spec direct(Name, Opts) -> Result
%%  Name = iolist()
%%  Opts = list()
%%  Result = ok | {error,Error}
%% @doc declare amqp direct exchange
direct(Name, Opts) ->
    call({direct, binary(Name), Opts}).

%% @spec topic(Name) -> Result
%%  Name = iolist()
%%  Result = ok | {error,Error}
%% @doc declare amqp topic exchange
topic(Name) ->
    call({topic, binary(Name)}).

%% @spec topic(Name, Opts) -> Result
%%  Name = iolist()
%%  Opts = list()
%%  Result = ok | {error,Error}
%% @doc declare amqp topic exchange
topic(Name, Opts) ->
    call({topic, binary(Name), Opts}).

%% @spec fanout(Name) -> Result
%%  Name = iolist()
%%  Result = ok | {error,Error}
%% @doc declare amqp fanout
fanout(Name) ->
    call({fanout, binary(Name)}).

%% @spec fanout(Name, Opts) -> Result
%%  Name = iolist()
%%  Opts = list()
%%  Result = ok | {error,Error}
%% @doc declare amqp fanout
fanout(Name, Opts) ->
    call({fanout, binary(Name), Opts}).

%% @spec bind(Exchange, Queue) -> Result
%%  Exchange = iolist()
%%  Queue = iolist()
%%  Result = ok | {error,Error}
%% @doc amqp bind
bind(Exchange, Queue) ->
    call({bind, binary(Exchange), binary(Queue)}).

%% @spec bind(Exchange, Queue, RouteKey) -> Result
%%  Exchange = iolist()
%%  Queue = iolist()
%%  Opts = list()
%%  Result = ok | {error,Error}
%% @doc amqp bind
bind(Exchange, Queue, RoutingKey) ->
    call({bind, binary(Exchange), binary(Queue), binary(RoutingKey)}).

%% @spec unbind(Exchange, Queue) -> Result
%%  Exchange = iolist()
%%  Queue = iolist()
%%  Result = ok | {error,Error}
%% @doc amqp bind
unbind(Exchange, Queue) ->
    call({unbind, binary(Exchange), binary(Queue)}).

%% @spec send(Queue, Payload) -> Result
%%  Queue = iolist()
%%  Payload = binary()
%% @doc send directly to queue
send(Queue, Payload) ->
    gen_server:cast(?MODULE, {send, binary(Queue), binary(Payload)}).

%% @spec publish(Exchange, Payload) -> Result
%%  Exchange = iolist()
%%  Payload = binary()
%% @doc amqp publish message
publish(Exchange, Payload) ->
    gen_server:cast(?MODULE, {publish, binary(Exchange), binary(Payload), <<"">>}).

%% @spec publish(Exchange, Payload, RoutingKey) -> Result
%%  Exchange = iolist()
%%  Payload = binary()
%%  RoutingKey = iolist()
%% @doc amqp publish message
publish(Exchange, Payload, RoutingKey) ->
    gen_server:cast(?MODULE, {publish, binary(Exchange), binary(Payload), binary(RoutingKey)}).

%% @spec publish(Exchange, Properties, Payload, RoutingKey) -> Result
%%  Exchange = iolist()
%%  Properties = [tuple()]
%%  Payload = binary()
%%  RoutingKey = iolist()
%% @doc amqp publish message
publish(Exchange, Properties, Payload, RoutingKey) ->
    gen_server:cast(?MODULE, {publish, binary(Exchange), Properties, binary(Payload), binary(RoutingKey)}).

%% @spec get(Queue) -> Result
%%  Queue = iolist()
%%  Result = ok | {error,Error}
%% @doc subscribe to a queue
get(Queue) ->
    call({get, binary(Queue)}).

%% @spec consume(Queue) -> Result
%%  Queue = iolist()
%%  Result = ok | {error,Error}
%% @doc subscribe to a queue
consume(Queue) ->
    call({consume, binary(Queue), self()}).

%% @spec consume(Queue, Consumer) -> Result
%%  Queue = iolist()
%%  Consumer = pid() | fun()
%%  Result = ok | {error,Error}
%% @doc subscribe to a queue
consume(Queue, Consumer) ->
    call({consume, binary(Queue), Consumer}).

%% @spec delete(queue, Queue) -> Result
%%  Queue = iolist()
%%  Result = ok | {error,Error}
%% @doc delete a queue
delete(queue, Queue) ->
    gen_server:call(?MODULE, {delete, queue, binary(Queue)});

%% @spec delete(exchange, Exchange) -> Result
%%  Exchange = iolist()
%%  Result = ok | {error,Error}
%% @doc delete an exchange
delete(exchange, Exchange) ->
    gen_server:call(?MODULE, {delete, exchange, binary(Exchange)}).

%% @spec ack(DeliveryTag) -> Result
%%  DeliveryTag = iolist()
%%  Result = ok | {error,Error}
%% @doc ack a message
ack(DeliveryTag) ->
    gen_server:cast(?MODULE, {ack, binary(DeliveryTag)}).

%% @spec cancel(ConsumerTag) -> Result
%%  ConsumerTag = iolist()
%%  Result = ok | {error,Error}
%% @doc cancel a consumer
cancel(ConsumerTag) ->
    gen_server:cast(?MODULE, {cancel, binary(ConsumerTag)}).

%%  close() -> Result
%%  close a channel
%close() ->
%    call(close).

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([Opts]) ->
    case do_init(Opts) of
    {ok, State} ->
        ets:new(queue, [set, protected, named_table]),
        ets:new(exchange, [set, protected, named_table]),
        ets:new(consumer, [set, protected, named_table]),
        io:format("~n~s: amqp is starting...[done]~n", [node()]),
        {ok, State};
    {error, Error} ->
        {stop, Error};
    {'EXIT', Reason} ->
        {stop, Reason}
    end.

do_init(Opts) ->
    process_flag(trap_exit, true),
    {value, Host} = dataset:get_value(host, Opts, "localhost"),
    {value, Port} = dataset:get_value(port, Opts, 5672),
    {value, VHost} = dataset:get_value(vhost, Opts, <<"/">>),
    {value, Realm} = dataset:get_value(realm, Opts, <<"/">>),
    {value, User} = dataset:get_value(user, Opts, <<"guest">>),
    {value, Password} = dataset:get_value(password, Opts, <<"guest">>),
    %%Start a connection to the server
    Params = #amqp_params{host = Host, port = Port, virtual_host = VHost, username = User, password = Password},
    {Conn, Chan, Ticket} = connect(Params, Realm),
    {ok, #state{params = Params, realm = Realm, ticket = Ticket, connection = Conn, channel = Chan}}.

connect(Params, Realm) ->
    Conn = amqp_connection:start_network_link(Params),
    link(Conn),
    %% Once you have a connection to the server, you can start an AMQP channel gain access to a realm
    Chan = amqp_connection:open_channel(Conn),
    Access = #'access.request'{realm = Realm,
                               exclusive = false,
                               passive = true,
                               active = true,
                               write = true,
                               read = true},
    #'access.request_ok'{ticket = Ticket} = amqp_channel:call(Chan, Access),
    {Conn, Chan, Ticket}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call(queue, _From, State) ->
    {ok, Q} = declare_queue(State),
    ets:insert(queue, {Q, true}),
    {reply, {ok, Q}, State};

handle_call({queue, Name}, _From, State) ->
    {ok, Q} = declare_queue(Name, [], State),
    ets:insert(queue, {Name, true}),
    {reply, {ok, Q}, State};

handle_call({queue, Name, Opts}, _From, State) ->
    {ok, Q} = declare_queue(Name, Opts, State),
    ets:insert(queue, {Name, true}),
    {reply, {ok, Q}, State};

handle_call({exchange, Name}, _From, State) ->
    declare_exchange(Name, <<"direct">>, [], State),
    ets:insert(exchange, {Name, <<"direct">>, []}),
    {reply, ok, State};

handle_call({exchange, Name, Type}, _From, State) ->
    declare_exchange(Name, Type, [], State),
    ets:insert(exchange, {Name, Type, []}),
    {reply, ok, State};

handle_call({exchange, Name, Type, Opts}, _From, State) ->
    declare_exchange(Name, Type, Opts, State),
    ets:insert(exchange, {Name, Type, Opts}),
    {reply, ok, State};

handle_call({direct, Name}, _From, State) ->   
    declare_exchange(Name, <<"direct">>, [], State),
    ets:insert(exchange, {Name, <<"direct">>, []}),
    {reply, ok, State};

handle_call({direct, Name, Opts}, _From, State) ->   
    declare_exchange(Name, <<"direct">>, Opts, State),
    ets:insert(exchange, {Name, <<"direct">>, Opts}),
    {reply, ok, State};

handle_call({topic, Name}, _From, State) ->   
    declare_exchange(Name, <<"topic">>, [], State),
    ets:insert(exchange, {Name, <<"topic">>, []}),
    {reply, ok, State};

handle_call({topic, Name, Opts}, _From, State) ->   
    declare_exchange(Name, <<"topic">>, Opts, State),
    ets:insert(exchange, {Name, <<"topic">>, Opts}),
    {reply, ok, State};

handle_call({fanout, Name}, _From, State) ->   
    declare_exchange(Name, <<"fanout">>, [], State),
    ets:insert(exchange, {Name, <<"fanout">>, []}),
    {reply, ok, State};

handle_call({fanout, Name, Opts}, _From, State) ->   
    declare_exchange(Name, <<"fanout">>, Opts, State),
    ets:insert(exchange, {Name, <<"fanout">>, Opts}),
    {reply, ok, State};

handle_call({bind, Exchange, Queue}, _From, State) -> 
    bind_queue(Exchange, Queue, <<"">>, State),
    {reply, ok, State};

handle_call({bind, Exchange, Queue, RoutingKey}, _From, State) -> 
    bind_queue(Exchange, Queue, RoutingKey, State),
    {reply, ok, State};

handle_call({unbind, Exchange, Queue}, _From, State) -> 
    unbind_queue(Exchange, Queue, <<"">>, State),
    {reply, ok, State};

handle_call({get, Queue}, _From, State) ->
    case basic_get(Queue, State) of
    {ok, Reply} -> 
        {reply, {ok, Reply}, State};
    {error, Reason} ->
        {reply, {error, Reason}, State}
    end;

handle_call({consume, Queue, Consumer}, _From, State) ->
    Reply = case basic_consume(Queue, State) of
    {ok, ConsumerTag} -> 
        MonRef = erlang:monitor(process, Consumer),
        ets:insert(consumer, {ConsumerTag, Consumer, MonRef}), 
        {ok, ConsumerTag};
    {error, Reason} ->
        {error, Reason}
    end,
    {reply, Reply, State};

handle_call({cancel, ConsumerTag}, _From, State) ->
    basic_cancel(ConsumerTag, State),
    {reply, ok, State};

handle_call({delete, queue, Queue}, _From, State) ->
    delete_queue(Queue, State),
    ets:delete(queue, Queue),
    {reply, ok, State};

handle_call({delete, exchange, Exchange}, _From, State) ->
    delete_exchange(Exchange, State),
    ets:delete(exchange, Exchange),
    {reply, ok, State};

handle_call(close, _From, #state{channel = Channel} = State) ->
    amqp_channel:close(Channel),
    {reply, ok, State#state{channel = undefined}};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(Req, _From, State) ->
    ?WARNING("unexpected request: ~p", [Req]),
    {reply, {error, unexpected_req}, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast({send, Queue, Payload}, State) ->
    basic_send(Queue, Payload, State),
    {noreply, State};

handle_cast({publish, Exchange, Payload, RoutingKey}, State) ->
    basic_publish(Exchange, none, Payload, RoutingKey, State),
    {noreply, State};

handle_cast({publish, Exchange, Properties, Payload, RoutingKey}, State) ->
    basic_publish(Exchange, Properties, Payload, RoutingKey, State),
    {noreply, State};

handle_cast({ack, DeliveryTag}, State) ->
    basic_ack(DeliveryTag, State),
    {noreply, State};

handle_cast(Msg, State) ->
    ?WARNING("unexpected msg: ~p", [Msg]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info({#'basic.deliver'{consumer_tag=ConsumerTag, 
    delivery_tag=_DeliveryTag, 
    redelivered=_Redelivered, 
    exchange=_Exchange, 
    routing_key=RoutingKey}, #amqp_msg{props = Properties, payload = Payload} = _Msg}, State) ->
    #'P_basic'{content_type = ContentType} = Properties,
    %?INFO("delivery got!"
    %        "~n from exchange: ~p" 
    %        "~n routing key: ~p"
    %        "~n content type: ~p", [Exchange, RoutingKey, ContentType]),
    case ets:lookup(consumer, ConsumerTag) of
    [{_Tag, Consumer, _Ref}] ->
        Consumer ! {deliver, RoutingKey, [{content_type, ContentType}], Payload};
    [] -> 
        ?WARNING("no available consumer for: ~p", [ConsumerTag])
    end,
    {noreply, State};

handle_info({'DOWN', MonRef, _Type, _Object, _Info}, State) ->
    case ets:match(consumer, {'$1', '_', MonRef}) of
    [] -> 
        ?WARNING("unexpected monitor down: ~p", [MonRef]);
    Consumers -> 
        lists:foreach(fun([Tag]) -> 
            basic_cancel(Tag, State), 
            ets:delete(consumer, Tag) 
        end, Consumers)
    end,
    {noreply, State};

handle_info(Info, State) ->
    ?WARNING("unexpected info: ~p", [Info]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, #state{connection = Conn} = _State) ->
    amqp_connection:close(Conn),
    ets:delete(queue),
    ets:delete(exchange),
    ets:delete(consumer),
    ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
call(Req) ->
    gen_server:call(?MODULE, Req).

declare_queue( #state{channel = Channel, ticket = Ticket}  = _State) ->
    QueueDeclare = #'queue.declare'{ticket = Ticket, auto_delete = true},
    #'queue.declare_ok'{queue = Q} = amqp_channel:call(Channel, QueueDeclare),
    {ok, Q}.

declare_queue(Name, Opts, #state{channel = Channel, ticket = Ticket}  = _State) ->
    Q = binary(Name),
    {value, Durable} = dataset:get_value(durable, Opts, false),
    {value, Exclusive} = dataset:get_value(exclusive, Opts, false),
    {value, AutoDelete} = dataset:get_value(auto_delete, Opts, true),
    QueueDeclare = #'queue.declare'{ticket = Ticket, queue = Q,
                                    passive = false, durable = Durable,
                                    exclusive = Exclusive, auto_delete = AutoDelete,
                                    nowait = false, arguments = []},
    #'queue.declare_ok'{queue = Q,
                        message_count = _MessageCount,
                        consumer_count = _ConsumerCount}
                        = amqp_channel:call(Channel, QueueDeclare),
    {ok, Q}.
    %io:format("~n~s: queue '~p' message_count: ~p~n", [node(), Name, MessageCount]),
    %io:format("~n~s: queue '~p' consumer_count: ~p~n", [node(), Name, ConsumerCount]).

declare_exchange(Name, Type, _Opts, #state{channel = Channel, ticket = Ticket} = _State) ->
    X = binary(Name),
    ExchangeDeclare = #'exchange.declare'{ticket = Ticket,
                                          exchange = X, type = Type,
                                          passive = false, durable = true,
                                          auto_delete = false, internal = false,
                                          nowait = false, arguments = []},
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, ExchangeDeclare).

bind_queue(Exchange, Queue, RoutingKey, #state{channel = Channel, ticket = Ticket} = _State) ->
    X = binary(Exchange),
    Q = binary(Queue),
    R = binary(RoutingKey),
    QueueBind = #'queue.bind'{ticket = Ticket,
                              queue = Q,
                              exchange = X,
                              routing_key = R,
                              nowait = false, arguments = []},
    #'queue.bind_ok'{} = amqp_channel:call(Channel, QueueBind).

unbind_queue(Exchange, Queue, RoutingKey, #state{channel = Channel, ticket = Ticket} = _State) ->
    X = binary(Exchange),
    Q = binary(Queue),
    QueueUnbind = #'queue.unbind'{ticket = Ticket,
                              queue = Q,
                              exchange = X,
                              routing_key = RoutingKey,
                              arguments = []},
    #'queue.unbind_ok'{} = amqp_channel:call(Channel, QueueUnbind).

basic_send(Queue, Payload0, #state{channel = Channel, ticket = Ticket} = _State) ->
    K = binary(Queue),
    Payload = binary(Payload0),
    BasicPublish = #'basic.publish'{ticket = Ticket,
                                    %exchange = X,
                                    routing_key = K,
                                    mandatory = false,
                                    immediate = false},
    Msg = #amqp_msg{props = basic_properties(), payload = Payload},
    amqp_channel:cast(Channel, BasicPublish, Msg).

basic_publish(Exchange, _Properties, Payload0, RoutingKey, #state{channel = Channel, ticket = Ticket} = _State) ->
    X = binary(Exchange),
    K = binary(RoutingKey),
    Payload = binary(Payload0),
    BasicPublish = #'basic.publish'{ticket = Ticket,
                                    exchange = X,
                                    routing_key = K,
                                    mandatory = false,
                                    immediate = false},
    Msg = #amqp_msg{props = basic_properties(), payload = Payload},

    amqp_channel:cast(Channel, BasicPublish, Msg).

basic_get(Queue, #state{channel = Channel, ticket = Ticket} = _State) ->
    Q = binary(Queue),
    %% Basic get
    BasicGet = #'basic.get'{ticket = Ticket, queue = Q},
    case amqp_channel:call(Channel, BasicGet) of
    {#'basic.get_ok'{exchange = _E, routing_key = RoutingKey, message_count = _C},  
     #'amqp_msg'{props = Properties, payload = Payload}} ->
        #'P_basic'{content_type = ContentType} = Properties,
        {ok, {RoutingKey, [{content_type, ContentType}], Payload}};
    #'basic.get_empty'{cluster_id = _} ->
        {ok, []}
    end.

basic_consume(Queue, #state{channel = Channel, ticket = Ticket} = _State) ->
    Q = binary(Queue),
    %% Register a consumer to listen to a queue
    BasicConsume = #'basic.consume'{ticket = Ticket,
                                    queue = Q,
                                    consumer_tag = <<"">>,
                                    no_local = false,
                                    no_ack = true,
                                    exclusive = false,
                                    nowait = false},
    #'basic.consume_ok'{consumer_tag = ConsumerTag}
                     = amqp_channel:subscribe(Channel, BasicConsume, self()),
    %% If the registration was sucessful, then consumer will be notified
    receive
    #'basic.consume_ok'{consumer_tag = ConsumerTag} -> 
        {ok, ConsumerTag}
    after 1000 -> 
        {error, consume_timeout}
    end.

basic_cancel(ConsumerTag, #state{channel = Channel} = _State) ->
    BasicCancel = #'basic.cancel'{consumer_tag = ConsumerTag, nowait = false},
    #'basic.cancel_ok'{consumer_tag = ConsumerTag} = amqp_channel:call(Channel,BasicCancel).

basic_ack(DeliveryTag, #state{channel = Channel} = _State) ->
    BasicAck = #'basic.ack'{delivery_tag = DeliveryTag},
    amqp_channel:cast(Channel,BasicAck).

delete_queue(Queue, #state{channel = Channel, ticket = Ticket} = _State) ->
    Q = binary(Queue),
    QueueDelete = #'queue.delete'{ticket = Ticket, queue = Q},
    #'queue.delete_ok'{message_count = _MessageCount} = amqp_channel:call(Channel, QueueDelete).

delete_exchange(Exchange, #state{channel = Channel, ticket = Ticket} = _State) ->
    X = binary(Exchange),
    ExchangeDelete = #'exchange.delete'{ticket = Ticket, exchange= X},
    #'exchange.delete_ok'{} = amqp_channel:call(Channel, ExchangeDelete).


binary(L) when is_list(L) ->
    list_to_binary(L);

binary(B) when is_binary(B) ->
    B.

basic_properties() ->
  #'P_basic'{content_type = <<"application/octet-stream">>,
             delivery_mode = 1,
             priority = 1}.
  

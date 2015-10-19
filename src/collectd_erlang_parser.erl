-module(collectd_erlang_parser).

%% API exports
-export([start/0]).
-export([loop/1]).

%%====================================================================
%% API functions
%%====================================================================



start() ->
  {ok, Socket} = gen_udp:open(1984, [binary, {active, false}]),
  spawn_link(?MODULE, loop, [Socket]),
  register(receiver, self()),
  receive
    stop -> gen_udp:close(Socket)
  end.

loop(Socket) ->
  case gen_udp:recv(Socket, 0) of
    {ok, {Ip, Port, Packet}} ->
      parse(Packet, []), loop(Socket)
  end.

parse(<<>>, LoopData) ->
  io:format("Packet: ~p~n", [LoopData]);
parse(Packet, LoopData) when is_binary(Packet) ->
  <<
  Type:16/big-unsigned-integer,
  LengthWithHeader:16/big-unsigned-integer,
  Rest/binary>> = Packet,
  Length = LengthWithHeader - 4,

  case Type of
%% host
    0 ->
      <<Host:Length/binary,
      NextPacket/binary>> = Rest,
      parse(NextPacket, [host, Host | LoopData]);
%% time
    1 ->
      <<Value:8/big-unsigned-integer-unit:8,
      NextPacket/binary>> = Rest,
      parse(NextPacket, [time, Value | LoopData]);
%% high res time
    8 ->
      <<Value:8/big-unsigned-integer-unit:8,
      NextPacket/binary>> = Rest,
      parse(NextPacket, [time, Value | LoopData]);
%% plugin
    2 ->
      <<Value:Length/binary,
      NextPacket/binary>> = Rest,
      parse(NextPacket, [plugin, Value | LoopData]);
%% plugin instance
    3 ->
      <<Value:Length/binary,
      NextPacket/binary>> = Rest,
      parse(NextPacket, [plugin_instance, Value | LoopData]);
%% type
    4 ->
      <<Value:Length/binary,
      NextPacket/binary>> = Rest,
      parse(NextPacket, [type, Value | LoopData]);
%% type instance
    5 ->
      <<Value:Length/binary,
      NextPacket/binary>> = Rest,
      parse(NextPacket, [type_instance, Value | LoopData]);
%% values
    6 ->
      <<Count:2/big-unsigned-integer-unit:8,
      RestWithTypesValues/binary
      >> = Rest,
      {RestWithValues, Types} = types(Count, RestWithTypesValues, []),
      {NextPacket, Values} = values(Types, RestWithValues, []),
      parse(NextPacket, [values, Values | LoopData]);
%% interval
    7 ->
      <<Value:8/big-unsigned-integer-unit:8,
      NextPacket/binary>> = Rest,
      parse(NextPacket, [interval, Value | LoopData]);
%% high res interval
    9 ->
      <<Value:8/big-unsigned-integer-unit:8,
      NextPacket/binary>> = Rest,
      parse(NextPacket, [interval, Value | LoopData]);

    _Other -> io:format("Parsed: ~p  type: ~B~n", [LoopData, Type])
  end.

types(0, PacketWithValues, Types) ->
  {PacketWithValues, Types};
types(Count, PacketWithValues, Types) ->
  <<Type:1/binary, Rest/binary>> = PacketWithValues,
  types(Count - 1, Rest, [Type | Types]).

values([], NextPacket, Values) ->
  {NextPacket, Values};
values([Type | Types], PacketWithValues, Values) ->
  case Type of
    <<0>> -> <<Value:8/big-unsigned-integer-unit:8, NextValue/binary>> = PacketWithValues;
    <<1>> -> <<Value:8/little-unsigned-float-unit:8, NextValue/binary>> = PacketWithValues;
    <<2>> -> <<Value:8/big-signed-integer-unit:8, NextValue/binary>> = PacketWithValues;
    <<3>> -> <<Value:8/big-unsigned-integer-unit:8, NextValue/binary>> = PacketWithValues
  end,
  values(Types, NextValue, [Value | Values]).
%%====================================================================
%% Internal functions
%%====================================================================

// Copyright (c) 2017, the gRPC project authors. Please see the AUTHORS file
// for details. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import 'dart:async';
import 'dart:convert';

import 'package:grpc/grpc.dart';
import 'package:grpc/src/client/channel.dart' as base;
import 'package:grpc/src/client/http2_connection.dart';
import 'package:grpc/src/shared/message.dart';
import 'package:http2/transport.dart';
import 'package:mockito/annotations.dart';
import 'package:mockito/mockito.dart';
import 'package:test/test.dart';

import 'client_utils.mocks.dart';
import 'utils.dart';

@GenerateMocks([ClientTransportConnection, ClientTransportStream])
class FakeConnection extends Http2ClientConnection {
  final ClientTransportConnection transport;

  var connectionError;

  FakeConnection(String host, this.transport, ChannelOptions options)
      : super(host, 443, options);

  @override
  Future<ClientTransportConnection> connectTransport() async {
    if (connectionError != null) throw connectionError;
    return transport;
  }
}

class FakeClientTransportConnection extends Http2ClientConnection {
  final ClientTransportConnector connector;

  var connectionError;

  FakeClientTransportConnection(this.connector, ChannelOptions options)
      : super.fromClientTransportConnector(connector, options);

  @override
  Future<ClientTransportConnection> connectTransport() async {
    if (connectionError != null) throw connectionError;
    return await connector.connect();
  }
}

Duration testBackoff(Duration? lastBackoff) => const Duration(milliseconds: 1);

class FakeChannelOptions implements ChannelOptions {
  @override
  ChannelCredentials credentials = const ChannelCredentials.secure();
  @override
  Duration idleTimeout = const Duration(seconds: 1);
  @override
  Duration connectionTimeout = const Duration(seconds: 10);
  @override
  String userAgent = 'dart-grpc/1.0.0 test';
  @override
  BackoffStrategy backoffStrategy = testBackoff;
  @override
  CodecRegistry codecRegistry = CodecRegistry.empty();
}

class FakeChannel extends ClientChannel {
  final Http2ClientConnection connection;
  @override
  final FakeChannelOptions options;

  FakeChannel(String host, this.connection, this.options)
      : super(host, options: options);

  @override
  Future<Http2ClientConnection> getConnection() async => connection;
}

class FakeClientConnectorChannel extends ClientTransportConnectorChannel {
  final Http2ClientConnection connection;
  @override
  final FakeChannelOptions options;

  FakeClientConnectorChannel(
      ClientTransportConnector connector, this.connection, this.options)
      : super(connector, options: options);

  @override
  Future<Http2ClientConnection> getConnection() async => connection;
}

typedef ServerMessageHandler = void Function(StreamMessage message);

class TestClient extends Client {
  late ClientMethod<int, int> _$unary;
  late ClientMethod<int, int> _$clientStreaming;
  late ClientMethod<int, int> _$serverStreaming;
  late ClientMethod<int, int> _$bidirectional;

  final int Function(List<int> value) decode;

  TestClient(base.ClientChannel channel,
      {CallOptions? options,
      Iterable<ClientInterceptor>? interceptors,
      this.decode = mockDecode})
      : super(channel, options: options, interceptors: interceptors) {
    _$unary = ClientMethod<int, int>('/Test/Unary', mockEncode, decode);
    _$clientStreaming =
        ClientMethod<int, int>('/Test/ClientStreaming', mockEncode, decode);
    _$serverStreaming =
        ClientMethod<int, int>('/Test/ServerStreaming', mockEncode, decode);
    _$bidirectional =
        ClientMethod<int, int>('/Test/Bidirectional', mockEncode, decode);
  }

  ResponseFuture<int> unary(int request, {CallOptions? options}) {
    return $createUnaryCall(_$unary, request, options: options);
  }

  ResponseFuture<int> clientStreaming(Stream<int> request,
      {CallOptions? options}) {
    return $createStreamingCall(_$clientStreaming, request, options: options)
        .single;
  }

  ResponseStream<int> serverStreaming(int request, {CallOptions? options}) {
    return $createStreamingCall(_$serverStreaming, Stream.value(request),
        options: options);
  }

  ResponseStream<int> bidirectional(Stream<int> request,
      {CallOptions? options}) {
    return $createStreamingCall(_$bidirectional, request, options: options);
  }
}

class ClientHarness extends _Harness {
  FakeConnection? connection;

  @override
  FakeChannel createChannel() {
    connection = FakeConnection('test', transport, channelOptions);
    return FakeChannel('test', connection!, channelOptions);
  }

  @override
  String get expectedAuthority => 'test';
}

class ClientTransportConnectorHarness extends _Harness {
  FakeClientTransportConnection? connection;
  late ClientTransportConnector connector;

  @override
  FakeClientConnectorChannel createChannel() {
    connector = FakeClientTransportConnector(transport);
    connection = FakeClientTransportConnection(connector, channelOptions);
    return FakeClientConnectorChannel(connector, connection!, channelOptions);
  }

  @override
  String get expectedAuthority => 'test';
}

class FakeClientTransportConnector extends ClientTransportConnector {
  final ClientTransportConnection _transportConnection;
  final completer = Completer();

  FakeClientTransportConnector(this._transportConnection);

  @override
  Future<ClientTransportConnection> connect() async => _transportConnection;

  @override
  String get authority => 'test';

  @override
  Future get done => completer.future;

  @override
  void shutdown() => completer.complete();
}

abstract class _Harness {
  late MockClientTransportConnection transport;
  late base.ClientChannel channel;
  late FakeChannelOptions channelOptions;
  late MockClientTransportStream stream;

  late StreamController<StreamMessage> fromClient;
  late StreamController<StreamMessage> toClient;

  Iterable<ClientInterceptor>? interceptors;

  bool headersWereSent = false;

  late TestClient client;

  base.ClientChannel createChannel();

  String get expectedAuthority;

  void setUp() {
    transport = MockClientTransportConnection();
    channelOptions = FakeChannelOptions();
    channel = createChannel();
    stream = MockClientTransportStream();
    fromClient = StreamController();
    toClient = StreamController();
    when(transport.makeRequest(any, endStream: anyNamed('endStream')))
        .thenReturn(stream);
    when(transport.onActiveStateChanged = captureAny).thenReturn(null);
    when(transport.isOpen).thenReturn(true);
    when(stream.outgoingMessages).thenReturn(fromClient.sink);
    when(stream.incomingMessages).thenAnswer((_) => toClient.stream);
    when(stream.terminate()).thenReturn(null);
    when(transport.finish()).thenAnswer((_) async {});
    client = TestClient(channel, interceptors: interceptors);
  }

  void tearDown() {
    fromClient.close();
    toClient.close();
  }

  static final _defaultHeaders = [
    Header.ascii(':status', '200'),
    Header.ascii('content-type', 'application/grpc'),
  ];

  static final _defaultTrailers = [
    Header.ascii('grpc-status', '0'),
  ];

  void sendResponseHeader() {
    assert(!headersWereSent);
    headersWereSent = true;
    toClient.add(HeadersStreamMessage(_defaultHeaders));
  }

  void sendResponseValue(int value) {
    toClient.add(DataStreamMessage(frame(mockEncode(value))));
  }

  void sendResponseTrailer({bool closeStream = true}) {
    toClient.add(HeadersStreamMessage([
      if (!headersWereSent) ..._defaultHeaders,
      ..._defaultTrailers,
    ], endStream: true));
    if (closeStream) toClient.close();
  }

  void signalIdle() {
    final ActiveStateHandler handler =
        verify(transport.onActiveStateChanged = captureAny).captured.single;
    expect(handler, isNotNull);
    handler(false);
  }

  Future<void> runTest(
      {Future? clientCall,
      dynamic expectedResult,
      String? expectedPath,
      Duration? expectedTimeout,
      Map<String, String>? expectedCustomHeaders,
      List<MessageHandler> serverHandlers = const [],
      void Function()? doneHandler,
      bool expectDone = true}) async {
    var serverHandlerIndex = 0;
    void handleServerMessage(StreamMessage message) {
      serverHandlers[serverHandlerIndex++](message);
    }

    final clientSubscription = fromClient.stream.listen(
        expectAsync1(handleServerMessage, count: serverHandlers.length),
        onError: expectAsync1((dynamic _) {}, count: 0),
        onDone: expectAsync0(doneHandler ?? () {}, count: expectDone ? 1 : 0));

    final result = await clientCall;
    if (expectedResult != null) {
      expect(result, expectedResult);
    }

    final List<Header> capturedHeaders =
        verify(transport.makeRequest(captureAny)).captured.single;
    validateRequestHeaders(
        Map.fromEntries(capturedHeaders.map((header) =>
            MapEntry(utf8.decode(header.name), utf8.decode(header.value)))),
        path: expectedPath,
        authority: expectedAuthority,
        timeout:
            expectedTimeout == null ? null : toTimeoutString(expectedTimeout),
        customHeaders: expectedCustomHeaders);

    await clientSubscription.cancel();
  }

  Future<void> expectThrows(
    Future? future,
    dynamic exception, {
    Map<String, String>? expectedCustomTrailers,
  }) async {
    try {
      await future;
      fail('Did not throw');
    } catch (e, st) {
      expect(e, exception);
      expect(st, isNot(equals(StackTrace.current)));
      if (expectedCustomTrailers != null) {
        if (e is GrpcError) {
          expect(e.trailers, expectedCustomTrailers);
        } else {
          fail('$e is not a GrpcError');
        }
      }
    }
  }

  Future<void> runFailureTest(
      {Future? clientCall,
      dynamic expectedException,
      String? expectedPath,
      Duration? expectedTimeout,
      Map<String, String>? expectedCustomHeaders,
      Map<String, String>? expectedCustomTrailers,
      List<MessageHandler> serverHandlers = const [],
      bool expectDone = true}) async {
    return runTest(
      clientCall: expectThrows(
        clientCall,
        expectedException,
        expectedCustomTrailers: expectedCustomTrailers,
      ),
      expectedPath: expectedPath,
      expectedTimeout: expectedTimeout,
      expectedCustomHeaders: expectedCustomHeaders,
      serverHandlers: serverHandlers,
      expectDone: expectDone,
    );
  }
}

// Copyright (c) 2017, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

import 'dart:async';
import 'dart:io';

import 'package:meta/meta.dart';
import 'package:http2/multiprotocol_server.dart';
import 'package:http2/transport.dart';

import 'streams.dart';

/// Definition of a gRPC service method.
class ServiceMethod<Q, R> {
  final String name;

  final bool streamingRequest;
  final bool streamingResponse;

  final Q Function(List<int> request) requestDeserializer;
  final List<int> Function(R response) responseSerializer;

  final Function handler;

  ServiceMethod(
      this.name,
      this.handler,
      this.streamingRequest,
      this.streamingResponse,
      this.requestDeserializer,
      this.responseSerializer);
}

/// Definition of a gRPC service.
abstract class Service {
  Map<String, ServiceMethod> _$methods = {};

  String get $name;

  void $addMethod(ServiceMethod method) {
    _$methods[method.name] = method;
  }

  ServiceMethod $lookupMethod(String name) => _$methods[name];
}

/// A gRPC server.
///
/// Listens for incoming gRPC calls, dispatching them to a [ServerHandler].
class Server {
  final Map<String, Service> _services = {};
  MultiProtocolHttpServer _server;

  SecurityContext _security;
  final int _port;

  Server({@required SecurityContext security, int port = 443})
      : _port = port,
        _security = security ?? new SecurityContext();

  void addService(Service service) {
    _services[service.$name] = service;
  }

  ServiceMethod _lookupMethod(String service, String method) =>
      _services[service]?.$lookupMethod(method);

  void _handleHttp11(HttpRequest request) {
    // TODO(jakobr): Handle upgrade to h2c, but only if insecure is allowed.
    request.response.statusCode = 505;
    request.response.write('HTTP/2 required.');
    request.response.close();
  }

  void _handleHttp2(ServerTransportStream stream) {
    new ServerHandler(_lookupMethod, stream).handle();
  }

  Future<Null> serve() async {
    _server = await MultiProtocolHttpServer.bind('0.0.0.0', _port, _security);
    _server.startServing(_handleHttp11, _handleHttp2, onError: (error, stack) {
      print('Error: $error');
    });
  }

  void shutdown() {
    _server?.close();
  }
}

/// Handles an incoming gRPC call.
class ServerHandler {
  final ServerTransportStream _stream;
  final ServiceMethod Function(String service, String method) _methodLookup;

  StreamSubscription<GrpcMessage> _incomingSubscription;

  Map<String, String> _clientMetadata;
  ServiceMethod _descriptor;

  StreamController _requests;
  bool _hasReceivedRequest = false;

  StreamSubscription _responseSubscription;
  bool _headersSent = false;

  ServerHandler(this._methodLookup, this._stream);

  void handle() {
    _incomingSubscription = _stream.incomingMessages
        .transform(new GrpcHttpDecoder())
        .transform(grpcDecompressor())
        .listen(_onDataIdle,
            onError: _onError, onDone: _onDoneError, cancelOnError: true);
  }

  // -- Idle state, incoming data --

  void _onDataIdle(GrpcMessage message) {
    if (message is! GrpcMetadata) {
      _sendError(401, 'Expected header frame');
      return;
    }
    final headerMessage = message
        as GrpcMetadata; // TODO(jakobr): Cast should not be necessary here.
    _clientMetadata = headerMessage.metadata;
    final path = _clientMetadata[':path'].split('/');
    if (path.length < 3) {
      _sendError(404, 'Invalid path');
      return;
    }
    final service = path[1];
    final method = path[2];
    _descriptor = _methodLookup(service, method);
    if (_descriptor == null) {
      _sendError(404, 'Method not found');
      return;
    }
    _startStreamingRequest();
  }

  void _startStreamingRequest() {
    _incomingSubscription.pause();
    _requests = new StreamController(
        onListen: _incomingSubscription.resume,
        onPause: _incomingSubscription.pause,
        onResume: _incomingSubscription.resume);
    _incomingSubscription.onData(_onDataActive);

    Stream responses;
    if (_descriptor.streamingResponse) {
      if (_descriptor.streamingRequest) {
        responses = _descriptor.handler(null, _requests.stream);
      } else {
        responses = _descriptor.handler(null, _requests.stream.single);
      }
    } else {
      Future response;
      if (_descriptor.streamingRequest) {
        response = _descriptor.handler(null, _requests.stream);
      } else {
        response = _descriptor.handler(null, _requests.stream.single);
      }
      responses = response.asStream();
    }
    _responseSubscription = responses.listen(_onResponse,
        onError: _onResponseError,
        onDone: _onResponseDone,
        cancelOnError: true);
    _incomingSubscription.onData(_onDataActive);
    _incomingSubscription.onDone(_onDoneExpected);
  }

  // -- Active state, incoming data --

  void _onDataActive(GrpcMessage message) {
    if (message is! GrpcData) {
      _sendError(711, 'Expected data frame');
      _requests
        ..addError(new GrpcError(712, 'No request received'))
        ..close();
      return;
    }

    if (_hasReceivedRequest && !_descriptor.streamingRequest) {
      _sendError(712, 'Too many requests');
      _requests
        ..addError(new GrpcError(712, 'Too many requests'))
        ..close();
    }

    var data =
        message as GrpcData; // TODO(jakobr): Cast should not be necessary here.
    var request;
    try {
      request = _descriptor.requestDeserializer(data.data);
    } catch (error) {
      _sendError(730, 'Error deserializing request: $error');
      _requests
        ..addError(new GrpcError(730, 'Error deserializing request: $error'))
        ..close();
      return;
    }
    _requests.add(request);
    _hasReceivedRequest = true;
  }

  // -- Active state, outgoing response data --

  void _onResponse(response) {
    _ensureHeadersSent();
    final bytes = _descriptor.responseSerializer(response);
    _stream.sendData(GrpcHttpEncoder.frame(bytes));
  }

  void _onResponseDone() {
    _sendTrailers();
  }

  void _onResponseError(error) {
    if (error is GrpcError) {
      // TODO(jakobr): error.metadata...
      _sendError(error.code, error.message);
    } else {
      _sendError(107, error.toString());
    }
  }

  void _ensureHeadersSent() {
    if (_headersSent) return;
    _sendHeaders();
  }

  void _sendHeaders() {
    if (_headersSent) throw new GrpcError(1514, 'Headers already sent');
    final headers = [
      new Header.ascii(':status',
          200.toString()), // TODO(jakobr): Should really be on package:http2.
      new Header.ascii('content-type', 'application/grpc'),
    ];
    // headers.addAll(context.headers);
    _stream.sendHeaders(headers);
    _headersSent = true;
  }

  void _sendTrailers({int status = 0, String message}) {
    final trailers = <Header>[];
    if (!_headersSent) {
      trailers.addAll([
        new Header.ascii(':status', 200.toString()),
        new Header.ascii('content-type', 'application/grpc'),
      ]);
    }
    trailers.add(new Header.ascii('grpc-status', status.toString()));
    if (message != null) {
      trailers.add(new Header.ascii('grpc-message', message));
    }
    // trailers.addAll(context.trailers);
    _stream.sendHeaders(trailers, endStream: true);
    // We're done!
    _incomingSubscription.cancel();
    _responseSubscription?.cancel();
  }

  // -- All states, incoming error / stream closed --

  void _onError(error) {
    print('Stream error: $error');
    // TODO(jakobr): Handle. Might be a cancel request from the client, which
    // should be propagated.
  }

  void _onDoneError() {
    _sendError(710, 'Request stream closed unexpectedly');
  }

  void _onDoneExpected() {
    if (!(_hasReceivedRequest || _descriptor.streamingRequest)) {
      _sendError(730, 'Expected request message');
      _requests.addError(new GrpcError(730, 'No request message received'));
    }
    _requests.close();
    _incomingSubscription.cancel();
  }

  void _sendError(int status, String message) {
    print('Sending error $status: $message');
    _sendTrailers(status: status, message: message);
  }
}

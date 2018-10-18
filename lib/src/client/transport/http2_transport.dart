// Copyright (c) 2018, the gRPC project authors. Please see the AUTHORS file
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
import 'dart:io';

import 'package:http2/transport.dart';
import 'package:meta/meta.dart';

import '../../shared/streams.dart';
import '../../shared/timeout.dart';

import '../options.dart';

import 'http2_credentials.dart';
import 'transport.dart';

class Http2TransportStream extends GrpcTransportStream {
  TransportStream _transportStream;
  StreamController<GrpcMessage> _incomingMessages;
  StreamController<List<int>> _outgoingMessages;

  Stream<GrpcMessage> get incomingMessages => _incomingMessages.stream;
  StreamSink<List<int>> get outgoingMessages => _outgoingMessages.sink;

  Http2TransportStream(this._transportStream) {
    _incomingMessages = new StreamController();
    _outgoingMessages = new StreamController();

    _transportStream.incomingMessages
        .transform(new GrpcHttpDecoder())
        .transform(grpcDecompressor())
        .pipe(_incomingMessages);

    _outgoingMessages.stream
        .map(GrpcHttpEncoder.frame)
        .map<StreamMessage>((bytes) => new DataStreamMessage(bytes))
        .handleError(_onRequestError)
        .listen(_transportStream.outgoingMessages.add,
            onError: _transportStream.outgoingMessages.addError,
            onDone: _transportStream.outgoingMessages.close,
            cancelOnError: true);
  }

  void _onRequestError() {
    // TODO: Implement errors on requests
  }

  @override
  Future<void> terminate() async {
    await _incomingMessages.close();
    await _outgoingMessages.close();
    _transportStream.terminate();
  }
}

class Http2Transport extends Transport {
  static final _methodPost = new Header.ascii(':method', 'POST');
  static final _schemeHttp = new Header.ascii(':scheme', 'http');
  static final _schemeHttps = new Header.ascii(':scheme', 'https');
  static final _contentTypeGrpc =
      new Header.ascii('content-type', 'application/grpc');
  static final _teTrailers = new Header.ascii('te', 'trailers');
  static final _grpcAcceptEncoding =
      new Header.ascii('grpc-accept-encoding', 'identity');
  static final _userAgent = new Header.ascii('user-agent', 'dart-grpc/0.2.0');

  final String host;
  final int port;
  final ChannelOptions options;

  @visibleForTesting
  ClientTransportConnection transport;

  Http2Transport(this.host, this.port, this.options);

  String get authority => options.credentials.authority ?? host;

  static List<Header> createCallHeaders(bool useTls, String authority,
      String path, Duration timeout, Map<String, String> metadata) {
    final headers = [
      _methodPost,
      useTls ? _schemeHttps : _schemeHttp,
      new Header(ascii.encode(':path'), utf8.encode(path)),
      new Header(ascii.encode(':authority'), utf8.encode(authority)),
    ];
    if (timeout != null) {
      headers.add(new Header.ascii('grpc-timeout', toTimeoutString(timeout)));
    }
    headers.addAll([
      _contentTypeGrpc,
      _teTrailers,
      _grpcAcceptEncoding,
      _userAgent,
    ]);
    metadata?.forEach((key, value) {
      headers.add(new Header(ascii.encode(key), utf8.encode(value)));
    });
    return headers;
  }

  @override
  Future<void> connect() async {
    var socket = await Socket.connect(host, port);

    final credentials = options.credentials;
    if (credentials is Http2ChannelCredentials) {
      final securityContext = credentials.securityContext;
      if (securityContext != null) {
        socket = await SecureSocket.secure(socket,
            host: authority,
            context: securityContext,
            onBadCertificate: _validateBadCertificate);
      }
    }
    socket.done.then(_handleSocketClosed);
    transport = ClientTransportConnection.viaSocket(socket);
  }

  @override
  GrpcTransportStream makeRequest(
      String path, Duration timeout, Map<String, String> metadata) {
    final headers = createCallHeaders(
        options.credentials.isSecure, authority, path, timeout, metadata);
    final stream = transport.makeRequest(headers);
    return new Http2TransportStream(stream);
  }

  @override
  Future<void> finish() async {
    await transport.finish();
  }

  @override
  Future<void> terminate() async {
    await transport.terminate();
  }

  bool _validateBadCertificate(X509Certificate certificate) {
    final credentials = options.credentials;
    if (credentials is Http2ChannelCredentials) {
      final validator = credentials.onBadCertificate;

      if (validator == null) return false;
      return validator(certificate, authority);
    }
    return false;
  }

  void _handleSocketClosed(_) {
    if (onSocketClosed != null) {
      onSocketClosed();
    }
  }
}

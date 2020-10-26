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
import 'dart:html';
import 'dart:typed_data';

import 'package:meta/meta.dart';

import '../../client/call.dart';
import '../../shared/message.dart';
import '../../shared/status.dart';
import '../connection.dart';
import 'cors.dart' as cors;
import 'transport.dart';
import 'web_streams.dart';

const _contentTypeKey = 'Content-Type';

/// All accepted content-type header's prefix.
const _validContentTypePrefix = [
  'application/grpc',
  'application/json+protobuf',
  'application/x-protobuf'
];

class XhrTransportStream implements GrpcTransportStream {
  final HttpRequest _request;
  final ErrorHandler _onError;
  final Function(XhrTransportStream stream) _onDone;
  int _requestBytesRead = 0;
  final StreamController<ByteBuffer> _incomingProcessor = StreamController();
  final StreamController<GrpcMessage> _incomingMessages = StreamController();
  final StreamController<List<int>> _outgoingMessages = StreamController();

  @override
  Stream<GrpcMessage> get incomingMessages => _incomingMessages.stream;

  @override
  StreamSink<List<int>> get outgoingMessages => _outgoingMessages.sink;

  XhrTransportStream(this._request, {onError, onDone})
      : _onError = onError,
        _onDone = onDone {
    _outgoingMessages.stream
        .map(frame)
        .listen((data) => _request.send(data), cancelOnError: true);

    _request.onReadyStateChange.listen((data) {
      if (_incomingMessages.isClosed) {
        return;
      }
      switch (_request.readyState) {
        case HttpRequest.HEADERS_RECEIVED:
          _onHeadersReceived();
          break;
        case HttpRequest.DONE:
          if (_request.status != 200) {
            _onError(
                GrpcError.unavailable(
                    'XhrConnection status ${_request.status}'),
                StackTrace.current);
          } else {
            _close();
          }
          break;
      }
    });

    _request.onError.listen((ProgressEvent event) {
      if (_incomingMessages.isClosed) {
        return;
      }
      _onError(GrpcError.unavailable('XhrConnection connection-error'),
          StackTrace.current);
      terminate();
    });

    _request.onProgress.listen((_) {
      if (_incomingMessages.isClosed) {
        return;
      }
      // Use response over responseText as most browsers don't support
      // using responseText during an onProgress event.
      final responseString = _request.response as String;
      final bytes = Uint8List.fromList(
              responseString.substring(_requestBytesRead).codeUnits)
          .buffer;
      _requestBytesRead = responseString.length;
      _incomingProcessor.add(bytes);
    });

    _incomingProcessor.stream
        .transform(GrpcWebDecoder())
        .transform(grpcDecompressor())
        .listen(_incomingMessages.add,
            onError: _onError, onDone: _incomingMessages.close);
  }

  bool _checkContentType(String contentType) {
    return _validContentTypePrefix.any(contentType.startsWith);
  }

  _onHeadersReceived() {
    final contentType = _request.getResponseHeader(_contentTypeKey);
    if (_request.status != 200) {
      _onError(GrpcError.unavailable('XhrConnection status ${_request.status}'),
          StackTrace.current);
      return;
    }
    if (contentType == null) {
      _onError(GrpcError.unavailable('XhrConnection missing Content-Type'),
          StackTrace.current);
      return;
    }
    if (!_checkContentType(contentType)) {
      _onError(
          GrpcError.unavailable('XhrConnection bad Content-Type $contentType'),
          StackTrace.current);
      return;
    }
    if (_request.response == null) {
      _onError(GrpcError.unavailable('XhrConnection request null response'),
          StackTrace.current);
      return;
    }

    // Force a metadata message with headers.
    final headers = GrpcMetadata(_request.responseHeaders);
    _incomingMessages.add(headers);
  }

  _close() {
    _incomingProcessor.close();
    _outgoingMessages.close();
    _onDone(this);
  }

  @override
  Future<void> terminate() async {
    _close();
    _request.abort();
  }
}

class XhrClientConnection extends ClientConnection {
  final Uri uri;

  final Set<XhrTransportStream> _requests = Set<XhrTransportStream>();

  XhrClientConnection(this.uri);

  String get authority => uri.authority;
  String get scheme => uri.scheme;

  void _initializeRequest(HttpRequest request, Map<String, String> metadata) {
    for (final header in metadata.keys) {
      request.setRequestHeader(header, metadata[header]);
    }
    // Overriding the mimetype allows us to stream and parse the data
    request.overrideMimeType('text/plain; charset=x-user-defined');
    request.responseType = 'text';
  }

  @visibleForTesting
  HttpRequest createHttpRequest() => HttpRequest();

  @override
  GrpcTransportStream makeRequest(String path, Duration timeout,
      Map<String, String> metadata, ErrorHandler onError,
      {CallOptions callOptions}) {
    // gRPC-web headers.
    if (_getContentTypeHeader(metadata) == null) {
      metadata['Content-Type'] = 'application/grpc-web+proto';
      metadata['X-User-Agent'] = 'grpc-web-dart/0.1';
      metadata['X-Grpc-Web'] = '1';
    }

    var requestUri = uri.resolve(path);
    if (callOptions is WebCallOptions &&
        callOptions.bypassCorsPreflight == true) {
      requestUri = cors.moveHttpHeadersToQueryParam(metadata, requestUri);
    }

    final HttpRequest request = createHttpRequest();
    request.open('POST', requestUri.toString());
    if (callOptions is WebCallOptions && callOptions.withCredentials == true) {
      request.withCredentials = true;
    }
    // Must set headers after calling open().
    _initializeRequest(request, metadata);

    final XhrTransportStream transportStream =
        XhrTransportStream(request, onError: onError, onDone: _removeStream);
    _requests.add(transportStream);
    return transportStream;
  }

  void _removeStream(XhrTransportStream stream) {
    _requests.remove(stream);
  }

  @override
  Future<void> terminate() async {
    for (XhrTransportStream request in _requests) {
      request.terminate();
    }
  }

  @override
  void dispatchCall(ClientCall call) {
    call.onConnectionReady(this);
  }

  @override
  Future<void> shutdown() async {}
}

MapEntry<String, String> _getContentTypeHeader(Map<String, String> metadata) {
  for (var entry in metadata.entries) {
    if (entry.key.toLowerCase() == _contentTypeKey.toLowerCase()) {
      return entry;
    }
  }
  return null;
}

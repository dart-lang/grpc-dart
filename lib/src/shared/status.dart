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

import 'package:grpc/src/generated/google/protobuf/any.pb.dart';
import 'package:grpc/src/generated/google/rpc/code.pbenum.dart';
import 'package:grpc/src/generated/google/rpc/error_details.pb.dart';
import 'package:protobuf/protobuf.dart';

class GrpcError implements Exception {
  final Code code;
  final String message;
  final List<GeneratedMessage> details;

  /// Custom error code.
  GrpcError.custom(this.code, [this.message, this.details]);

  /// The operation completed successfully.
  GrpcError.ok([this.message, this.details]) : code = Code.OK;

  /// The operation was cancelled (typically by the caller).
  GrpcError.cancelled([this.message, this.details]) : code = Code.CANCELLED;

  /// Unknown error. An example of where this error may be returned is if a
  /// Status value received from another address space belongs to an error-space
  /// that is not known in this address space. Also errors raised by APIs that
  /// do not return enough error information may be converted to this error.
  GrpcError.unknown([this.message, this.details]) : code = Code.UNKNOWN;

  /// Client specified an invalid argument. Note that this differs from
  /// [failedPrecondition]. [invalidArgument] indicates arguments that are
  /// problematic regardless of the state of the system (e.g., a malformed file
  /// name).
  GrpcError.invalidArgument([this.message, this.details])
      : code = Code.INVALID_ARGUMENT;

  /// Deadline expired before operation could complete. For operations that
  /// change the state of the system, this error may be returned even if the
  /// operation has completed successfully. For example, a successful response
  /// from a server could have been delayed long enough for the deadline to
  /// expire.
  GrpcError.deadlineExceeded([this.message, this.details])
      : code = Code.DEADLINE_EXCEEDED;

  /// Some requested entity (e.g., file or directory) was not found.
  GrpcError.notFound([this.message, this.details]) : code = Code.NOT_FOUND;

  /// Some entity that we attempted to create (e.g., file or directory) already
  /// exists.
  GrpcError.alreadyExists([this.message, this.details])
      : code = Code.ALREADY_EXISTS;

  /// The caller does not have permission to execute the specified operation.
  /// [permissionDenied] must not be used for rejections caused by exhausting
  /// some resource (use [resourceExhausted] instead for those errors).
  /// [permissionDenied] must not be used if the caller cannot be identified
  /// (use [unauthenticated] instead for those errors).
  GrpcError.permissionDenied([this.message, this.details])
      : code = Code.PERMISSION_DENIED;

  /// Some resource has been exhausted, perhaps a per-user quota, or perhaps the
  /// entire file system is out of space.
  GrpcError.resourceExhausted([this.message, this.details])
      : code = Code.RESOURCE_EXHAUSTED;

  /// Operation was rejected because the system is not in a state required for
  /// the operation's execution. For example, directory to be deleted may be
  /// non-empty, an rmdir operation is applied to a non-directory, etc.
  ///
  /// A litmus test that may help a service implementor in deciding between
  /// [failedPrecondition], [aborted], and [unavailable]:
  /// (a) Use [unavailable] if the client can retry just the failing call.
  /// (b) Use [aborted] if the client should retry at a higher-level (e.g.,
  ///     restarting a read-modify-write sequence).
  /// (c) Use [failedPrecondition] if the client should not retry until the
  ///     system state has been explicitly fixed.  E.g., if an "rmdir" fails
  ///     because the directory is non-empty, [failedPrecondition] should be
  ///     returned since the client should not retry unless they have first
  ///     fixed up the directory by deleting files from it.
  GrpcError.failedPrecondition([this.message, this.details])
      : code = Code.FAILED_PRECONDITION;

  /// The operation was aborted, typically due to a concurrency issue like
  /// sequencer check failures, transaction aborts, etc.
  ///
  /// See litmus test above for deciding between [failedPrecondition],
  /// [aborted], and [unavailable].
  GrpcError.aborted([this.message, this.details]) : code = Code.ABORTED;

  /// Operation was attempted past the valid range. E.g., seeking or reading
  /// past end of file.
  ///
  /// Unlike invalidArgument, this error indicates a problem that may be fixed
  /// if the system state changes. For example, a 32-bit file system will
  /// generate invalidArgument if asked to read at an offset that is not in the
  /// range [0,2^32-1], but it will generate [outOfRange] if asked to read from
  /// an offset past the current file size.
  ///
  /// There is a fair bit of overlap between [failedPrecondition] and
  /// [outOfRange]. We recommend using [outOfRange] (the more specific error)
  /// when it applies so that callers who are iterating through a space can
  /// easily look for an [outOfRange] error to detect when they are done.
  GrpcError.outOfRange([this.message, this.details]) : code = Code.OUT_OF_RANGE;

  /// Operation is not implemented or not supported/enabled in this service.
  GrpcError.unimplemented([this.message, this.details])
      : code = Code.UNIMPLEMENTED;

  /// Internal errors. Means some invariants expected by underlying system has
  /// been broken. If you see one of these errors, something is very broken.
  // TODO(sigurdm): This should probably not be an [Exception].
  GrpcError.internal([this.message, this.details]) : code = Code.INTERNAL;

  /// The service is currently unavailable.  This is a most likely a transient
  /// condition and may be corrected by retrying with a backoff.
  ///
  /// See litmus test above for deciding between [failedPrecondition],
  /// [aborted], and [unavailable].
  GrpcError.unavailable([this.message, this.details]) : code = Code.UNAVAILABLE;

  /// Unrecoverable data loss or corruption.
  GrpcError.dataLoss([this.message, this.details]) : code = Code.DATA_LOSS;

  /// The request does not have valid authentication credentials for the
  /// operation.
  GrpcError.unauthenticated([this.message, this.details])
      : code = Code.UNAUTHENTICATED;

  @override
  bool operator ==(other) {
    if (other is! GrpcError) return false;
    return code == other.code && message == other.message;
  }

  @override
  int get hashCode => code.hashCode ^ (message?.hashCode ?? 17);

  @override
  String toString() =>
      'gRPC Error (${code.value} ${code.name}, $message, $details)';
}

/// Parse error details into the right kind of GeneratedMessage.
GeneratedMessage parseGeneratedMessage(Any any) {
  switch (any.typeUrl) {
    case 'type.googleapis.com/google.rpc.RetryInfo':
      return RetryInfo.fromBuffer(any.value);

    case 'type.googleapis.com/google.rpc.DebugInfo':
      return DebugInfo.fromBuffer(any.value);

    case 'type.googleapis.com/google.rpc.QuotaFailure':
      return QuotaFailure.fromBuffer(any.value);

    case 'type.googleapis.com/google.rpc.ErrorInfo':
      return ErrorInfo.fromBuffer(any.value);

    case 'type.googleapis.com/google.rpc.PreconditionFailure':
      return PreconditionFailure.fromBuffer(any.value);

    case 'type.googleapis.com/google.rpc.BadRequest':
      return BadRequest.fromBuffer(any.value);

    case 'type.googleapis.com/google.rpc.RequestInfo':
      return RequestInfo.fromBuffer(any.value);

    case 'type.googleapis.com/google.rpc.ResourceInfo':
      return ResourceInfo.fromBuffer(any.value);

    case 'type.googleapis.com/google.rpc.Help':
      return Help.fromBuffer(any.value);

    case 'type.googleapis.com/google.rpc.LocalizedMessage':
      return LocalizedMessage.fromBuffer(any.value);

    default:
      return any;
  }
}

package io.temporal.worker.packages.acceptance;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.MethodDescriptor;
import io.temporal.api.workflowservice.v1.RespondWorkflowTaskCompletedRequest;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.testing.TestEnvironmentOptions;
import io.temporal.testing.TestWorkflowEnvironment;

/** Public-API configuration for the pinned Java test server's normal-queue execution mode. */
final class TestServices {
  private TestServices() {}

  static TestWorkflowEnvironment environment() {
    return TestWorkflowEnvironment.newInstance(
        TestEnvironmentOptions.newBuilder()
            .setWorkflowServiceStubsOptions(options().build())
            .build());
  }

  static WorkflowServiceStubsOptions.Builder options() {
    return WorkflowServiceStubsOptions.newBuilder()
        .addGrpcClientInterceptor(new NormalQueueInterceptor());
  }

  /**
   * The pinned test server stores sticky queues but does not expire their schedule-to-start timeout
   * or implement ResetStickyTaskQueue. After a worker dies those queues are otherwise unreachable.
   * Decline sticky execution through the public transport API. Every new worker reconstructs from
   * real full history; all application commands, version markers, payloads, and headers are
   * untouched. This interceptor belongs only to acceptance/demo service clients, never to the
   * production plugin.
   */
  private static final class NormalQueueInterceptor implements ClientInterceptor {
    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> method, CallOptions options, Channel next) {
      return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
          next.newCall(method, options)) {
        @Override
        @SuppressWarnings("unchecked")
        public void sendMessage(ReqT message) {
          if (message instanceof RespondWorkflowTaskCompletedRequest) {
            RespondWorkflowTaskCompletedRequest completed =
                (RespondWorkflowTaskCompletedRequest) message;
            super.sendMessage((ReqT) completed.toBuilder().clearStickyAttributes().build());
          } else {
            super.sendMessage(message);
          }
        }
      };
    }
  }
}

package io.temporal.worker.packages.acceptance;

import static org.junit.Assert.*;

import com.google.protobuf.ByteString;
import io.temporal.api.common.v1.Payload;
import io.temporal.common.interceptors.Header;
import io.temporal.worker.packages.api.PackageRef;
import io.temporal.worker.packages.runtime.PackageHeaders;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class HeaderContractTest {
  private static final String DIGEST = TestSupport.repeat("a", 64);

  @Test
  public void mergePreservesOtherPayloadBytesAndOriginalHeader() {
    Payload tracing =
        Payload.newBuilder()
            .putMetadata("encoding", ByteString.copyFromUtf8("binary/plain"))
            .setData(ByteString.copyFrom(new byte[] {0, -1, 2, 3}))
            .build();
    Map<String, Payload> original = new HashMap<>(TestSupport.map("traceparent", tracing));
    Header workflow = PackageHeaders.workflow(new Header(original), "orders");
    assertEquals("orders", PackageHeaders.workflowPackageId(workflow));
    assertSame(tracing, workflow.getValues().get("traceparent"));
    assertEquals(
        "caller headers are not mutated", TestSupport.map("traceparent", tracing), original);
    PackageRef ref = new PackageRef("orders", 2, DIGEST);
    Header activity = PackageHeaders.activity(new Header(original), ref);
    assertEquals(ref, PackageHeaders.activityRef(activity));
    assertEquals(tracing.toByteString(), activity.getValues().get("traceparent").toByteString());
    assertEquals(
        "idempotent composition",
        activity.getValues(),
        PackageHeaders.activity(activity, ref).getValues());
    assertThrows(
        RuntimeException.class,
        () -> PackageHeaders.activity(activity, new PackageRef("orders", 12, DIGEST)));
    assertThrows(RuntimeException.class, () -> PackageHeaders.workflow(workflow, "billing"));
  }

  @Test
  public void activityHeaderRequiresExactReferenceAndStrictSchema() {
    assertThrows(RuntimeException.class, () -> PackageHeaders.activityRef(Header.empty()));
    for (String version :
        java.util.Arrays.asList(
            "2", "0", "release-candidate", "adf0fc802850d5a35fc5ea34de8c9afbfddb6d6f")) {
      PackageRef ref = new PackageRef("orders", version, DIGEST);
      assertEquals(ref, PackageHeaders.activityRef(PackageHeaders.activity(Header.empty(), ref)));
    }
    for (String invalid :
        java.util.Arrays.asList(
            "{}",
            "null",
            "[]",
            "not-json",
            "{\"formatVersion\":2,\"packageId\":\"orders\",\"packageVersion\":\"2\",\"sha256\":\""
                + DIGEST
                + "\"}",
            "{\"formatVersion\":1,\"packageId\":\"orders\",\"packageVersion\":\"-1\",\"sha256\":\""
                + DIGEST
                + "\"}",
            "{\"formatVersion\":1,\"packageId\":\"orders\",\"packageVersion\":\"\",\"sha256\":\""
                + DIGEST
                + "\"}",
            "{\"formatVersion\":1,\"packageId\":\"orders\",\"packageVersion\":2.5,\"sha256\":\""
                + DIGEST
                + "\"}",
            "{\"formatVersion\":1,\"packageId\":\"orders\",\"packageVersion\":2147483648,\"sha256\":\""
                + DIGEST
                + "\"}",
            "{\"formatVersion\":1,\"packageId\":\"orders\",\"packageVersion\":0,\"sha256\":\""
                + DIGEST
                + "\"}",
            "{\"formatVersion\":1,\"packageId\":\"../orders\",\"packageVersion\":\"2\",\"sha256\":\""
                + DIGEST
                + "\"}",
            "{\"formatVersion\":1,\"packageId\":\"orders\",\"packageVersion\":\"2\",\"sha256\":\"short\"}",
            "{\"formatVersion\":1,\"packageId\":\"orders\",\"packageVersion\":\"2\",\"packageVersion\":\"12\",\"sha256\":\""
                + DIGEST
                + "\"}",
            "{\"formatVersion\":1,\"packageId\":\"orders\",\"packageVersion\":\"2\",\"sha256\":\""
                + DIGEST
                + "\",\"extra\":true}")) {
      assertThrows(
          invalid, RuntimeException.class, () -> PackageHeaders.activityRef(json(invalid)));
    }
    assertThrows(
        RuntimeException.class,
        () -> PackageHeaders.activityRef(PackageHeaders.workflow(Header.empty(), "orders")));
    assertThrows(
        RuntimeException.class,
        () ->
            PackageHeaders.workflowPackageId(
                PackageHeaders.activity(Header.empty(), new PackageRef("orders", 2, DIGEST))));
    Payload binary =
        Payload.newBuilder()
            .putMetadata("encoding", ByteString.copyFromUtf8("binary/plain"))
            .setData(ByteString.copyFromUtf8("{}"))
            .build();
    assertThrows(
        RuntimeException.class,
        () -> PackageHeaders.activityRef(new Header(TestSupport.map(PackageHeaders.KEY, binary))));
  }

  private static Header json(String json) {
    Payload payload =
        Payload.newBuilder()
            .putMetadata("encoding", ByteString.copyFromUtf8("json/plain"))
            .setData(ByteString.copyFromUtf8(json))
            .build();
    return new Header(TestSupport.map(PackageHeaders.KEY, payload));
  }
}

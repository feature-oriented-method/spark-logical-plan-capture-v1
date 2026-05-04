package io.github.mt.logicplan

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import java.util.Base64
import java.nio.charset.StandardCharsets
import scala.util.matching.Regex

/**
 * Spark extension that injects a logical plan capture rule.
 */
class LogicalPlanCaptureExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectPostHocResolutionRule { spark => new LogicalPlanCaptureRule }
  }
}

/**
 * Rule that captures the logical plan and logs it as a Base64-encoded JSON payload.
 */
class LogicalPlanCaptureRule extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    val queryId = java.util.UUID.randomUUID().toString
    val event = LogicalPlanCaptureRule.buildEvent(plan, queryId)
    logInfo(event.asLogLine)
    plan
  }
}

object LogicalPlanCaptureRule {
  private val Marker: String = "SPARK_LOGICAL_PLAN_CAPTURE_V1"
  private val EventRegex: Regex = """^(\S+)\s+queryId=([^\s]+)\s+payloadBase64=([^\s]+)\s*$""".r

  /**
   * Build a capture event from a logical plan and query ID.
   */
  def buildEvent(plan: LogicalPlan, queryId: String): CaptureEvent = {
    val json = serializePlan(plan)
    val payloadBase64 = Base64.getEncoder.encodeToString(json.getBytes(StandardCharsets.UTF_8))
    CaptureEvent(Marker, queryId, payloadBase64)
  }

  /**
   * Decode a Base64-encoded payload back to JSON string.
   */
  def decodePayload(payloadBase64: String): String = {
    new String(Base64.getDecoder.decode(payloadBase64), StandardCharsets.UTF_8)
  }

  /**
   * Parse a log line into a CaptureEvent if it matches the expected format.
   */
  def parseLogLine(line: String): Option[CaptureEvent] = {
    Option(line).flatMap {
      case EventRegex(marker, queryId, payloadBase64) =>
        Some(CaptureEvent(marker, queryId, payloadBase64))
      case _ => None
    }
  }

  /**
   * Serialize a logical plan to JSON, falling back to treeString on failure.
   */
  private def serializePlan(plan: LogicalPlan): String = {
    try {
      plan.toJSON
    } catch {
      case _: Throwable => plan.treeString
    }
  }

  /**
   * Case class representing a captured logical plan event.
   */
  case class CaptureEvent(marker: String, queryId: String, payloadBase64: String) {
    def asLogLine: String = {
      s"$marker queryId=$queryId payloadBase64=$payloadBase64"
    }
  }
}

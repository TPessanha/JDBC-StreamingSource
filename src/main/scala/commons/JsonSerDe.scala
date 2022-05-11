package commons

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}

import java.io.{InputStream, OutputStream}

final class JsonSerDe(objectMapper: ObjectMapper with ScalaObjectMapper) extends SerDe {

  def writeAsPrettyString(value: Any): String =
    objectMapper.writerWithDefaultPrettyPrinter.writeValueAsString(value)

  override def writeAsBytes(value: Any): Array[Byte] =
    objectMapper.writeValueAsBytes(value)

  override def writeAsString(value: Any): String =
    objectMapper.writeValueAsString(value)

  override def write(value: Any, stream: OutputStream): Unit =
    objectMapper.writeValue(stream, value)

  override def read[T](data: Array[Byte])(implicit manifest: Manifest[T]): T =
    objectMapper.readValue(data)(manifest)

  override def read[T](data: String)(implicit manifest: Manifest[T]): T =
    objectMapper.readValue(data)(manifest)

  override def read[T](data: InputStream)(implicit manifest: Manifest[T]): T =
    objectMapper.readValue(data)(manifest)

  override def read[T](json: String, typeReference: TypeReference[T]): T =
    objectMapper.readValue(json, typeReference)

  override def readTree(content: String): JsonNode =
    objectMapper.readTree(content)

  override def convertValue[T](value: Any, `class`: Class[T]): T =
    objectMapper.convertValue[T](value, `class`)
}

object JsonSerDe {

  private val DefaultInstance = new JsonSerDe(createDefaultMapper())

  def default(): JsonSerDe = DefaultInstance

  def createDefaultMapper(): ObjectMapper with ScalaObjectMapper = {
    val objectMapper = new ObjectMapper with ScalaObjectMapper
    objectMapper.enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
    objectMapper.enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    objectMapper.enable(DeserializationFeature.WRAP_EXCEPTIONS)
    objectMapper.setSerializationInclusion(JsonInclude.Include.ALWAYS)
    objectMapper.disable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE)
    objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    objectMapper.registerModule(new JavaTimeModule)
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper
  }
}

package commons

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.JsonNode

import java.io.{InputStream, OutputStream}
import scala.util.{Failure, Success, Try}

trait SerDe {

  def writeAsBytes(value: Any): Array[Byte]

  def writeAsString(value: Any): String

  def write(value: Any, stream: OutputStream): Unit

  def read[T](data: Array[Byte])(implicit manifest: Manifest[T]): T

  def read[T](data: String)(implicit manifest: Manifest[T]): T

  def read[T](data: InputStream)(implicit manifest: Manifest[T]): T

  final def tryRead[T](data: Array[Byte])(implicit manifest: Manifest[T]): Either[Throwable, T] =
    Try(read(data)(manifest)) match {
      case Success(value) => Right(value)
      case Failure(t) => Left(t)
    }

  final def tryRead[T](data: String)(implicit manifest: Manifest[T]): Either[Throwable, T] =
    Try(read(data)(manifest)) match {
      case Success(value) => Right(value)
      case Failure(t) => Left(t)
    }

  final def tryRead[T](data: InputStream)(implicit manifest: Manifest[T]): Either[Throwable, T] =
    Try(read(data)(manifest)) match {
      case Success(value) => Right(value)
      case Failure(t) => Left(t)
    }

  def read[T](json: String, typeReference: TypeReference[T]): T

  def readTree(content: String): JsonNode

  def convertValue[T](value: Any, `class`: Class[T]): T
}
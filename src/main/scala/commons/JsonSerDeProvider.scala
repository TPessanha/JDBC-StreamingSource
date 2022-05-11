package commons

/**
 * [[JsonSerDeProvider]] provides a wrapper for [[JsonSerDe]] serializer/deserializer class.
 */
trait JsonSerDeProvider {

  protected lazy val serde: JsonSerDe = JsonSerDe.default()

}

package spark.sql.streaming.sources.offsets

import org.apache.spark.sql.types._
import utils.UnitSpec

class JdbcOffsetSpec extends UnitSpec {

    "JdbcOffset" should "compare INTEGER types" in {
        val offsetSmall = JdbcOffset("1", IntegerType)
        val offsetBig = JdbcOffset("5", IntegerType)

        assert(offsetSmall.compare(offsetBig) == -1)

        assert(offsetBig.compare(offsetBig) == 0)
        assert(offsetSmall.compare(offsetSmall) == 0)

        assert(offsetBig.compare(offsetSmall) == 1)
    }

    it should "compare BYTE types" in {
        val offsetSmall = JdbcOffset("1", ByteType)
        val offsetBig = JdbcOffset("5", ByteType)

        assert(offsetSmall.compare(offsetBig) == -1)

        assert(offsetBig.compare(offsetBig) == 0)
        assert(offsetSmall.compare(offsetSmall) == 0)

        assert(offsetBig.compare(offsetSmall) == 1)
    }

    it should "compare SHORT types" in {
        val offsetSmall = JdbcOffset("1", ShortType)
        val offsetBig = JdbcOffset("5", ShortType)

        assert(offsetSmall.compare(offsetBig) == -1)

        assert(offsetBig.compare(offsetBig) == 0)
        assert(offsetSmall.compare(offsetSmall) == 0)

        assert(offsetBig.compare(offsetSmall) == 1)
    }

    it should "compare LONG types" in {
        val offsetSmall = JdbcOffset("1", LongType)
        val offsetBig = JdbcOffset("5", LongType)

        assert(offsetSmall.compare(offsetBig) == -1)

        assert(offsetBig.compare(offsetBig) == 0)
        assert(offsetSmall.compare(offsetSmall) == 0)

        assert(offsetBig.compare(offsetSmall) == 1)
    }

    it should "compare FLOAT types" in {
        val offsetSmall = JdbcOffset("1.5", FloatType)
        val offsetBig = JdbcOffset("5.5", FloatType)

        assert(offsetSmall.compare(offsetBig) == -1)

        assert(offsetBig.compare(offsetBig) == 0)
        assert(offsetSmall.compare(offsetSmall) == 0)

        assert(offsetBig.compare(offsetSmall) == 1)
    }

    it should "compare DOUBLE types" in {
        val offsetSmall = JdbcOffset("1.5", DoubleType)
        val offsetBig = JdbcOffset("5.5", DoubleType)

        assert(offsetSmall.compare(offsetBig) == -1)

        assert(offsetBig.compare(offsetBig) == 0)
        assert(offsetSmall.compare(offsetSmall) == 0)

        assert(offsetBig.compare(offsetSmall) == 1)
    }

    it should "compare DATE types" in {
        val offsetSmall = JdbcOffset("2022-01-01", DateType)
        val offsetBig = JdbcOffset("2022-01-02", DateType)

        assert(offsetSmall.compare(offsetBig) == -1)

        assert(offsetBig.compare(offsetBig) == 0)
        assert(offsetSmall.compare(offsetSmall) == 0)

        assert(offsetBig.compare(offsetSmall) == 1)
    }

    it should "compare TIMESTAMP types" in {
        val offsetSmall = JdbcOffset("2022-01-01", DateType)
        val offsetBig = JdbcOffset("2022-01-02", DateType)

        assert(offsetSmall.compare(offsetBig) == -1)

        assert(offsetBig.compare(offsetBig) == 0)
        assert(offsetSmall.compare(offsetSmall) == 0)

        assert(offsetBig.compare(offsetSmall) == 1)
    }

    it should "compare DECIMAl types" in {
        val offsetSmall = JdbcOffset("1.6", DecimalType(1, 1))
        val offsetBig = JdbcOffset("5.5", DecimalType(1, 1))

        assert(offsetSmall.compare(offsetBig) == -1)

        assert(offsetBig.compare(offsetBig) == 0)
        assert(offsetSmall.compare(offsetSmall) == 0)

        assert(offsetBig.compare(offsetSmall) == 1)
    }

    it should "throw error when comparing different data types" in {
        val offsetSmall = JdbcOffset("1", IntegerType)
        val offsetBig = JdbcOffset("5.5", FloatType)

        assertThrows[IllegalArgumentException](offsetSmall.compare(offsetBig) == -1)
    }

}

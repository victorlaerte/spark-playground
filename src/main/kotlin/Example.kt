import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext

/**
 * @author Victor Oliveira
 */
fun main(args: Array<String>) {
//	example1()
	example2()
}

fun example1() {
	runSpark {
		val lines = getResourceTextFile("busline.txt")
		println("YOUR FILE HAS ${lines.count()} lines")
	}
}

fun example2() {
	runSpark {
		getResourceTextFile("busline.txt")
			.filter {
				it.contains("JD.MARIA.LUIZA")
			}
			.collect()
			.forEach {
				print(it)
			}
	}
}

fun example3() {
	runSpark {
		getResourceTextFile("busline.txt")
			.filter {
				it.contains("JD.MARIA.LUIZA")
			}
			.collect()
			.forEach {
				print(it)
			}
	}
}

inline fun runSpark(block: JavaSparkContext.() -> Unit) {
	init().also { block(it) }.close()
}

fun init(): JavaSparkContext {
	return JavaSparkContext(
		SparkConf().apply {
			setMaster("local")
			setAppName("BusProcessor")
		})
}

fun JavaSparkContext.getResourceTextFile(fileName: String): JavaRDD<String> {
	return this.textFile("src/main/resources/$fileName")
}


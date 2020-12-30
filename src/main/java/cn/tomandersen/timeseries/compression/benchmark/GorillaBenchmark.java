package cn.tomandersen.timeseries.compression.benchmark;

import org.openjdk.jmh.annotations.*;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * <h3>Gorilla Benchmark.</h3>
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/11/26
 */
@BenchmarkMode(Mode.Throughput)
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 10) // Reduce the amount of iterations if you start to see GC interference
public class GorillaBenchmark {
    public static class DataGenerator {

        // Block的起始时间戳,默认为当前时间截断至Hour
        public long blockStartTS = Instant.now().truncatedTo(ChronoUnit.HOURS).toEpochMilli();

        // 准备测试数据集
        public void setup() {

        }
    }
}

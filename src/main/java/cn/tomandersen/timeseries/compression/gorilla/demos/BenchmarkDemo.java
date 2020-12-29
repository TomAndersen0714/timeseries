package cn.tomandersen.timeseries.compression.gorilla.demos;

import fi.iki.yak.ts.compression.gorilla.benchmark.EncodingBenchmark;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * <h3>用于Gorilla Benchmark</h3>
 * 基于Java JMH(Java Micro-benchmark Harness)基准测试工具对指定接口进行性能测试.
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/11/26
 */
public class BenchmarkDemo {
    public static void main(String[] args) throws RunnerException {
        Options opts = new OptionsBuilder().include(EncodingBenchmark.class.getSimpleName())
                .output("C:\\Users\\DELL\\Desktop\\benchmark.log").build();
        new Runner(opts).run();
    }
}

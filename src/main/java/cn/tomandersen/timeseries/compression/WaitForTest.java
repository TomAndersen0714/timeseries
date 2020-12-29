package cn.tomandersen.timeseries.compression;

import java.lang.annotation.*;

/**
 * This annotation indicates that the method need to be tested.
 */
@Documented
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.METHOD)
@interface WaitForTest {
}

package cn.tomandersen.timeseries.compression;

import java.lang.reflect.Array;
import java.util.Arrays;

/**
 * Demo about java.lang.reflect.*.
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/12/28
 */
public class ReflectTest {

    /**
     * This method attempt to generate a new array and copy all elements in old one.
     *
     * @param oldArray  the array to copy
     * @param newLength the length of new array
     * @return a new array that contains all elements of old one.However, the array to return has
     * type Object[] which can't be converted to original type.
     * @date 2020/12/28
     */
    public static Object[] badCopyOf(Object[] oldArray, int newLength) {
        // Construct a new array with new length.
        Object[] newArray = new Object[newLength];
        // Copy elements from old array to new one in which the type is Object[].
        System.arraycopy(oldArray, 0, newArray, 0, Math.min(oldArray.length, newLength));
        return newArray;
    }

    /**
     * This method generates an array by allocating a new array of the same type and
     * copying all elements.
     *
     * @param oldArray  the array to copy
     * @param newLength the length of new array
     * @return a new array that contains all elements of old one.For the convenience of
     * converting type, returned type is Object.
     * @date 2020/12/28
     */
    public static Object goodCopyOf(Object oldArray, int newLength) {
        // Get the class, and judge whether it is a array object.
        Class oldArrayClass = oldArray.getClass();
        if (!oldArrayClass.isArray()) return null;
        // Get the type of element in old array, and generate a new array.
        Class componentType = oldArrayClass.getComponentType();
        int oldLength = Array.getLength(oldArray);
        Object newArray = Array.newInstance(componentType, newLength);
        // Copy all elements in old array .
        System.arraycopy(oldArray, 0, newArray, 0, Math.min(oldLength, newLength));
        return newArray;

    }

    public static void main(String[] args) {

        int[] a = new int[]{1, 2, 3};
        a = (int[]) goodCopyOf(a, 10);
        System.out.println(Arrays.toString(a));


        String[] b = new String[]{"Tom", "Alis", "Harry"};
        b = (String[]) goodCopyOf(b, 10);
        System.out.println(Arrays.toString(b));

        // 以下代码会抛出 java.lang.ClassCastException 异常
        // 即 Object[] 类型转换成对象类型数组时，虽然编译时不会报错，但是运行时会
        // 抛出异常“java.lang.ClassCastException”
//        b = (String[]) badCopyOf(b, 10);
    }
}

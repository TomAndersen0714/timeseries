package cn.tomandersen.timeseries.compression.benchmark;

/**
 * @author TomAndersen
 * @version 1.0
 * @date 2020/12/5
 */
public class Experiment {
    public static void main(String[] args) {
        String path = "C:\\Users\\DELL\\Desktop\\TSDataset\\with timestamps\\with abnormal timestamp\\ATimeSeriesDataset-master\\";
//        String dataset = "Server\\Server32";
//        String dataset = "UCR\\HandOutlines";
//        String dataset = "IoT\\IoT1";
//        String dataset = "tmp\\testDataset";
        String[] timestampDataset = new String[]{
                "IoT\\IoT0", "IoT\\IoT1", "IoT\\IoT2", "IoT\\IoT3", "IoT\\IoT4", "IoT\\IoT5", "IoT\\IoT6", "IoT\\IoT7",
//                "Server\\Server30", "Server\\Server31", "Server\\Server32", "Server\\Server34", "Server\\Server35",
//                "Server\\Server41", "Server\\Server43", "Server\\Server46", "Server\\Server47", "Server\\Server48",
        };
        String[] metricValueDatasetA = new String[]{
                "IoT\\IoT1", "IoT\\IoT2", "IoT\\IoT5", "IoT\\IoT7",
                "Server\\Server30", "Server\\Server32", "Server\\Server35", "Server\\Server43",
                "Server\\Server47", "Server\\Server48",
                "UCR\\Haptics", "UCR\\UWaveGestureLibraryAll", "UCR\\HandOutlines", "UCR\\StarLightCurves"
        };

        String[] metricValueDatasetB = new String[]{
                "Server\\Server57", "Server\\Server62", "Server\\Server66", "Server\\Server77",
                "Server\\Server82", "Server\\Server94", "Server\\Server97", "Server\\Server106",
                "Server\\Server109", "Server\\Server115",
                "UCR\\Phoneme", "UCR\\InlineSkate", "UCR\\MALLAT", "UCR\\CinC_ECG_torso"
        };

        String[] serverDataset = new String[]{
                "Server\\Server30", "Server\\Server32", "Server\\Server35",
                "Server\\Server43", "Server\\Server47", "Server\\Server48",
                "Server\\Server57", "Server\\Server62", "Server\\Server66",
                "Server\\Server77", "Server\\Server82", "Server\\Server94",
                "Server\\Server97", "Server\\Server106", "Server\\Server109",
                "Server\\Server115"
        };

        String[] UCRDataset = new String[]{
                "UCR\\CinC_ECG_torso", "UCR\\InlineSkate", "UCR\\MALLAT", "UCR\\Phoneme",
                "UCR\\Haptics", "UCR\\UWaveGestureLibraryAll", "UCR\\HandOutlines",
                "UCR\\StarLightCurves"
        };

        String[] integerValueDatasets = new String[]{
                "tmp\\Server35", "tmp\\Server43", "tmp\\Server47", "tmp\\Server48",
                "tmp\\Server62", "tmp\\Server77", "tmp\\Server82", "tmp\\Server97",
                "tmp\\Server106", "tmp\\Server115"
        };

        for (String dataset : serverDataset) {
            System.out.println("---------");
            System.out.println(dataset);
            GorillaCompressionDemo.compressionDemo(path + dataset, false);
            System.out.println();
            APECompressionDemo.compressionDemo(path + dataset, false);
        }

    }
}

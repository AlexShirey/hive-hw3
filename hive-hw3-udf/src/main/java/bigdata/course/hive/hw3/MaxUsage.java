package bigdata.course.hive.hw3;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MaxUsage extends UDAF {

    /**
     * The actual class for doing the aggregation. Hive will automatically look
     * for all internal classes of the UDAF that implements UDAFEvaluator.
     */
    public static class MaxUsageEvaluator implements UDAFEvaluator {

        private InnerData data = null;

        public MaxUsageEvaluator() {
            super();
            init();
        }

        static class InnerData {
            HashMap<String, Long> deviceMap = new HashMap<>();
            HashMap<String, Long> osMap = new HashMap<>();
            HashMap<String, Long> browserMap = new HashMap<>();
            HashMap<String, Long> uaMap = new HashMap<>();
        }

        @Override
        public void init() {
            data = new InnerData();
        }

        /**
         * Iterate through one row of original data.
         */
        public boolean iterate(final String UAColumn) throws HiveException {

            if (UAColumn == null) {
                throw new HiveException("the value is null");
            }

            UserAgent userAgent = UserAgent.parseUserAgentString(UAColumn);

            String device = userAgent.getOperatingSystem().getDeviceType().getName();
            String os = userAgent.getOperatingSystem().getName();
            String browser = userAgent.getBrowser().getName();
            String ua = userAgent.getBrowser().getBrowserType().getName();

            doIterate(data.deviceMap, device);
            doIterate(data.osMap, os);
            doIterate(data.browserMap, browser);
            doIterate(data.uaMap, ua);

            return true;
        }


        /**
         * Puts to the inner map the name of the device, os, browser and counts how often this name meets in records
         */
        private void doIterate(HashMap<String, Long> map, String name) {

            Long count;
            if ((count = map.get(name)) == null) {
                map.put(name, 1L);
            } else {
                count++;
                map.put(name, count);
            }
        }


        /**
         * Terminate a partial aggregation and return the data (to Reducers).
         */
        public InnerData terminatePartial() {

            return data;
        }

        /**
         * Reducer's task - merge data that come from Mappers.
         * <p>
         * This function should always have a single argument which has the same
         * type as the return value of terminatePartial().
         * <p>
         * This function should always return true.
         */
        public boolean merge(InnerData otherData) {

            if (otherData == null) {
                return true;
            }

            doMerge(data.deviceMap, otherData.deviceMap);
            doMerge(data.osMap, otherData.osMap);
            doMerge(data.browserMap, otherData.browserMap);
            doMerge(data.uaMap, otherData.uaMap);

            return true;
        }

        /**
         * Summarizes the count for every name.
         */
        private void doMerge(HashMap<String, Long> currentMap, HashMap<String, Long> otherMap) {

            Set<String> names = otherMap.keySet();

            for (String name : names) {
                Long currentCount = currentMap.get(name);
                Long otherCount = otherMap.get(name);
                if (currentCount == null) {
                    currentCount = 0L;
                }
                Long totalCount = currentCount + otherCount;
                currentMap.put(name, totalCount);
            }
        }


        /**
         * Terminates the aggregation and return the final result.
         */
        public HashMap<String, String> terminate() {

            HashMap<String, String> returnMap = new HashMap<>();

            doTerminate(data.deviceMap, "device", returnMap);
            doTerminate(data.osMap, "os", returnMap);
            doTerminate(data.browserMap, "browser", returnMap);
            doTerminate(data.uaMap, "ua", returnMap);

            return returnMap;
        }


        /**
         * Gets the name with max count (meets more often then others),
         * and puts to the returnMap this name and its count.
         */
        private void doTerminate(HashMap<String, Long> map, String groupName, HashMap<String, String> returnMap) {

            String nameWithMaxCount = null;
            Long maxCount = 0L;

            Set<Map.Entry<String, Long>> entries = map.entrySet();

            for (Map.Entry<String, Long> entry : entries) {
                if (entry.getValue() > maxCount) {
                    nameWithMaxCount = entry.getKey();
                    maxCount = entry.getValue();
                }
            }

            returnMap.put(groupName, nameWithMaxCount + " (" + maxCount + ")");
        }


    }
}

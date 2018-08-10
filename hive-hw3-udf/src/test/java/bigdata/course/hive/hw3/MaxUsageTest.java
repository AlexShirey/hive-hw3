package bigdata.course.hive.hw3;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class MaxUsageTest {

    private MaxUsage.MaxUsageEvaluator mapper1 = new MaxUsage.MaxUsageEvaluator();
    private MaxUsage.MaxUsageEvaluator mapper2 = new MaxUsage.MaxUsageEvaluator();
    private MaxUsage.MaxUsageEvaluator reducer = new MaxUsage.MaxUsageEvaluator();

    @Test
    public void maxUsageTest() throws HiveException {

        String record1 = "Mozilla/5.0 (Windows NT 5.1; rv:24.0) Gecko/20100101 Firefox/24.0";
        String record2 = "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 SE 2.X MetaSr 1.0";
        String record3 = "Mozilla/5.0 (Windows NT 6.1; rv:24.0) Gecko/20100101 Firefox/24.0";
        String record4 = "Mozilla/5.0 (Windows NT 6.1; rv:24.0) Gecko/20100101 Firefox/24.0";

        mapper1.iterate(record1);
        mapper1.iterate(record2);
        mapper1.iterate(record3);
        mapper1.iterate(record4);

        String record2_1 = "tMozilla/5.0 (Windows NT 5.1) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.57 Safari/537.17 SE 2.X M";
        String record2_2 = "Mozilla/5.0 (iPad; U; CPU OS 3_2 like Mac OS X; en-us) AppleWebKit/531.21.10 (KHTML, like Gecko) Version/4.0.4 Mobile/7B367 Safari/531.21.10";
        String record2_3 = "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)";
        String record2_4 = "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; SV1; qdesk 2.4.1263.203)";

        mapper2.iterate(record2_1);
        mapper2.iterate(record2_2);
        mapper2.iterate(record2_3);
        mapper2.iterate(record2_4);

        MaxUsage.MaxUsageEvaluator.InnerData innerData1 = mapper1.terminatePartial();
        MaxUsage.MaxUsageEvaluator.InnerData innerData2 = mapper2.terminatePartial();

        reducer.merge(innerData1);
        reducer.merge(innerData2);

        HashMap<String, String> output = reducer.terminate();

        Assert.assertNotNull(output.get("device"));
        Assert.assertNotNull(output.get("os"));
        Assert.assertNotNull(output.get("browser"));
        Assert.assertNotNull(output.get("ua"));

        String deviceExpected = "Computer (7)";
        String osExpected = "Windows XP (4)";
        String browserExpected = "Firefox 24 (3)";
        String uaExpected = "Browser (7)";

        Assert.assertEquals(deviceExpected, output.get("device"));
        Assert.assertEquals(osExpected, output.get("os"));
        Assert.assertEquals(browserExpected, output.get("browser"));
        Assert.assertEquals(uaExpected, output.get("ua"));

    }

}
package com.folkol;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.Test;

import static org.junit.Assert.*;

public class SimpleDBTest {
    private String db = "tmp/" + UUID.randomUUID().toString();

    @Test
    public void store() throws Exception {
        SimpleDB<Double> target = new SimpleDB<>(db, Double.class);

        String key = UUID.randomUUID().toString();

        Double oldValue = target.put(key, Math.PI);
        assertNull(oldValue);

        Path path = Paths.get(String.format("%s/data/%s", db, key));
        assertTrue(Files.exists(path));

        String value = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
        assertEquals(Math.PI, Double.parseDouble(value), 0.01);
    }

    @Test
    public void overwrite() throws Exception {
        SimpleDB<Double> target = new SimpleDB<>(db, Double.class);
        String key = UUID.randomUUID().toString();

        target.put(key, Math.PI);
        Double oldValue = target.put(key, 0.0);

        assertNotNull(oldValue);
        assertEquals(Math.PI, oldValue, 0.01);
    }

    @Test
    public void wrongType() throws Exception {
        SimpleDB<Double> first = new SimpleDB<>(db, Double.class);
        try {
            SimpleDB<Map> second = new SimpleDB<>(db, Map.class);
        } catch (RuntimeException e) {
            String message = e.getMessage();
            assertTrue("Expected failed type assertion, got: " + message, message.contains("Stored type ("));
        }
    }

    @Test
    public void searchAsStream() throws Exception {
        SimpleDB<TestBean> target = new SimpleDB<>(db, TestBean.class);

        for (String name : Arrays.asList("foo", "bar", "baz", "qux")) {
            target.put(name, new TestBean(name));
        }

        long numAs = target.values()
                           .stream()
                           .filter(bean -> bean.getS1().contains("a"))
                           .count();
        assertEquals(2, numAs);
    }

    @Test
    public void storeAndRetrieve() throws Exception {
        SimpleDB<TestBean> target = new SimpleDB<>(db, TestBean.class);
        TestBean testBean = new TestBean();

        target.put("foo", testBean);
        TestBean retrieved = target.get("foo");

        assertEquals(testBean, retrieved);
    }

    static class WTF extends HashMap<String, Map<String, List<Map<String, List<TestBean>>>>> {

    }

    @Test
    public void storeAndRetrieveDeepStructure() throws Exception {
        String key = UUID.randomUUID().toString();
        SimpleDB<WTF> target = new SimpleDB<>(db, WTF.class);

        WTF wtf = new WTF();

        String outerKey = "foo";
        TestBean foo = new TestBean();
        TestBean bar = new TestBean();

        List<TestBean> innerList = new ArrayList<>();
        innerList.add(foo);
        innerList.add(bar);

        Map<String, List<TestBean>> innerMap = new HashMap<>();
        innerMap.put("baz", innerList);

        List<Map<String, List<TestBean>>> outerList = new ArrayList<>();
        outerList.add(innerMap);

        Map<String, List<Map<String, List<TestBean>>>> outerMap = new HashMap<>();
        outerMap.put("qux", outerList);

        wtf.put(outerKey, outerMap);

        target.put(key, wtf);

        WTF loaded = target.get(key);

        assertEquals(wtf, loaded);
    }

    static class TestBean {
        private String s1, s2, s3;

        public TestBean() {
            this.s1 = UUID.randomUUID().toString();
            this.s2 = UUID.randomUUID().toString();
            this.s3 = UUID.randomUUID().toString();
        }

        public TestBean(String s1) {
            setS1(s1);
        }

        public String getS1() {
            return s1;
        }

        public void setS1(String s1) {
            this.s1 = s1;
        }

        public String getS2() {
            return s2;
        }

        public void setS2(String s2) {
            this.s2 = s2;
        }

        public String getS3() {
            return s3;
        }

        public void setS3(String s3) {
            this.s3 = s3;
        }

        @Override
        public String toString() {
            return "TestBean{" +
                "s1='" + s1 + '\'' +
                ", s2='" + s2 + '\'' +
                ", s3='" + s3 + '\'' +
                '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TestBean testBean = (TestBean) o;

            if (s1 != null ? !s1.equals(testBean.s1) : testBean.s1 != null) return false;
            if (s2 != null ? !s2.equals(testBean.s2) : testBean.s2 != null) return false;
            return s3 != null ? s3.equals(testBean.s3) : testBean.s3 == null;
        }

        @Override
        public int hashCode() {
            int result = s1 != null ? s1.hashCode() : 0;
            result = 31 * result + (s2 != null ? s2.hashCode() : 0);
            result = 31 * result + (s3 != null ? s3.hashCode() : 0);
            return result;
        }
    }
}

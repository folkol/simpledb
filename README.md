# SimpleDB

Super simple "database" for when you really do not care that much.

(A Persistent Map that serializes objects with a bundled Gson.)

## Basic usage

```java
SimpleDB<TestBean> target = new SimpleDB<>(db, TestBean.class);
TestBean testBean = new TestBean();

target.put("foo", testBean);
TestBean retrieved = target.get("foo");
```

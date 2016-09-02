/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.tests.utils;

import java.util.*;

import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.tests.load.Generator;
import org.apache.ignite.tests.pojos.ProductOrder;
import org.apache.ignite.tests.pojos.Person;
import org.apache.ignite.tests.pojos.PersonId;
import org.apache.ignite.tests.pojos.Product;
import org.springframework.core.io.ClassPathResource;

/**
 * Helper class for all tests
 */
public class TestsHelper {
    /** */
    private static final String LETTERS_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    /** */
    private static final String NUMBERS_ALPHABET = "0123456789";

    /** */
    private static final Random RANDOM = new Random(System.currentTimeMillis());

    /** */
    private static final ResourceBundle TESTS_SETTINGS = ResourceBundle.getBundle("tests");

    /** */
    private static final int BULK_OPERATION_SIZE = parseTestSettings("bulk.operation.size");

    /** */
    private static final String LOAD_TESTS_CACHE_NAME = TESTS_SETTINGS.getString("load.tests.cache.name");

    /** */
    private static final int LOAD_TESTS_THREADS_COUNT = parseTestSettings("load.tests.threads.count");

    /** */
    private static final int LOAD_TESTS_WARMUP_PERIOD = parseTestSettings("load.tests.warmup.period");

    /** */
    private static final int LOAD_TESTS_EXECUTION_TIME = parseTestSettings("load.tests.execution.time");

    /** */
    private static final int LOAD_TESTS_REQUESTS_LATENCY = parseTestSettings("load.tests.requests.latency");

    /** */
    private static final String LOAD_TESTS_PERSISTENCE_SETTINGS = TESTS_SETTINGS.getString("load.tests.persistence.settings");

    /** */
    private static final String LOAD_TESTS_IGNITE_CONFIG = TESTS_SETTINGS.getString("load.tests.ignite.config");

    private static final int LOAD_TESTS_YEAR;
    private static final int LOAD_TESTS_MONTH;
    private static final int LOAD_TESTS_DAY;

    private static final int LOAD_TESTS_PRODUCT_MIN = parseTestSettings("load.tests.product.min");
    private static final int LOAD_TESTS_PRODUCT_MAX = parseTestSettings("load.tests.product.max");
    private static final int LOAD_TESTS_PRODUCT_ABS_MIN = parseTestSettings("load.tests.product.abs.min");
    private static final int LOAD_TESTS_PRODUCT_ABS_MAX = parseTestSettings("load.tests.product.abs.max");

    /** */
    private static final Generator LOAD_TESTS_KEY_GENERATOR;

    /** */
    private static final Generator LOAD_TESTS_VALUE_GENERATOR;

    /** */
    private static int parseTestSettings(String name) {
        return Integer.parseInt(TESTS_SETTINGS.getString(name));
    }

    static {
        Calendar cl = Calendar.getInstance();

        String year = TESTS_SETTINGS.getString("load.tests.year");
        LOAD_TESTS_YEAR = !year.trim().isEmpty() ? Integer.parseInt(year) : cl.get(Calendar.YEAR);

        String month = TESTS_SETTINGS.getString("load.tests.month");
        LOAD_TESTS_MONTH = !month.trim().isEmpty() ? Integer.parseInt(month) : cl.get(Calendar.MONTH);

        String day = TESTS_SETTINGS.getString("load.tests.day");
        LOAD_TESTS_DAY = !day.trim().isEmpty() ? Integer.parseInt(day) : cl.get(Calendar.DAY_OF_MONTH);

        try {
            LOAD_TESTS_KEY_GENERATOR = (Generator)Class.forName(TESTS_SETTINGS.getString("load.tests.key.generator")).newInstance();
            LOAD_TESTS_VALUE_GENERATOR = (Generator)Class.forName(TESTS_SETTINGS.getString("load.tests.value.generator")).newInstance();
        }
        catch (Throwable e) {
            throw new RuntimeException("Failed to initialize TestsHelper", e);
        }
    }

    /** */
    public static int getLoadTestsThreadsCount() {
        return LOAD_TESTS_THREADS_COUNT;
    }

    /** */
    public static int getLoadTestsWarmupPeriod() {
        return LOAD_TESTS_WARMUP_PERIOD;
    }

    /** */
    public static int getLoadTestsExecutionTime() {
        return LOAD_TESTS_EXECUTION_TIME;
    }

    /** */
    public static int getLoadTestsRequestsLatency() {
        return LOAD_TESTS_REQUESTS_LATENCY;
    }

    /** */
    public static ClassPathResource getLoadTestsPersistenceSettings() {
        return new ClassPathResource(LOAD_TESTS_PERSISTENCE_SETTINGS);
    }

    /** */
    public static String getLoadTestsIgniteConfig() {
        return LOAD_TESTS_IGNITE_CONFIG;
    }

    /** */
    public static int getBulkOperationSize() {
        return BULK_OPERATION_SIZE;
    }

    /** */
    public static String getLoadTestsCacheName() {
        return LOAD_TESTS_CACHE_NAME;
    }

    /** */
    public static Object generateLoadTestsKey(long i) {
        return LOAD_TESTS_KEY_GENERATOR.generate(i);
    }

    /** */
    public static Object generateLoadTestsValue(long i) {
        return LOAD_TESTS_VALUE_GENERATOR.generate(i);
    }

    public static int getLoadTestsYear() {
        return LOAD_TESTS_YEAR;
    }

    public static int getLoadTestsMonth() {
        return LOAD_TESTS_MONTH;
    }

    public static int getLoadTestsDay() {
        return LOAD_TESTS_DAY;
    }

    public static int getLoadTestsProductMin() {
        return LOAD_TESTS_PRODUCT_MIN;
    }

    public static int getLoadTestsProductMax() {
        return LOAD_TESTS_PRODUCT_MAX;
    }

    public static int getLoadTestsProductAbsMin() {
        return LOAD_TESTS_PRODUCT_ABS_MIN;
    }

    public static int getLoadTestsProductAbsMax() {
        return LOAD_TESTS_PRODUCT_ABS_MAX;
    }

    /** */
    @SuppressWarnings("unchecked")
    public static CacheEntryImpl generateLoadTestsEntry(long i) {
        return new CacheEntryImpl(TestsHelper.generateLoadTestsKey(i), TestsHelper.generateLoadTestsValue(i));
    }

    /** */
    public static <K, V> Collection<K> getKeys(Collection<CacheEntryImpl<K, V>> entries) {
        List<K> list = new LinkedList<>();

        for (CacheEntryImpl<K, ?> entry : entries)
            list.add(entry.getKey());

        return list;
    }

    /** */
    public static Map<Long, Long> generateLongsMap() {
        return generateLongsMap(BULK_OPERATION_SIZE);
    }

    /** */
    public static Map<Long, Long> generateLongsMap(int cnt) {
        Map<Long, Long> map = new HashMap<>();

        for (long i = 0; i < cnt; i++)
            map.put(i, i + 123);

        return map;
    }

    /** */
    public static Collection<CacheEntryImpl<Long, Long>> generateLongsEntries() {
        return generateLongsEntries(BULK_OPERATION_SIZE);
    }

    /** */
    public static Collection<CacheEntryImpl<Long, Long>> generateLongsEntries(int cnt) {
        Collection<CacheEntryImpl<Long, Long>> entries = new LinkedList<>();

        for (long i = 0; i < cnt; i++)
            entries.add(new CacheEntryImpl<>(i, i + 123));

        return entries;
    }

    /** */
    public static Map<String, String> generateStringsMap() {
        return generateStringsMap(BULK_OPERATION_SIZE);
    }

    /** */
    public static Map<String, String> generateStringsMap(int cnt) {
        Map<String, String> map = new HashMap<>();

        for (int i = 0; i < cnt; i++)
            map.put(Integer.toString(i), randomString(5));

        return map;
    }

    /** */
    public static Collection<CacheEntryImpl<String, String>> generateStringsEntries() {
        return generateStringsEntries(BULK_OPERATION_SIZE);
    }

    /** */
    public static Collection<CacheEntryImpl<String, String>> generateStringsEntries(int cnt) {
        Collection<CacheEntryImpl<String, String>> entries = new LinkedList<>();

        for (int i = 0; i < cnt; i++)
            entries.add(new CacheEntryImpl<>(Integer.toString(i), randomString(5)));

        return entries;
    }

    /** */
    public static Map<Long, Person> generateLongsPersonsMap() {
        Map<Long, Person> map = new HashMap<>();

        for (long i = 0; i < BULK_OPERATION_SIZE; i++)
            map.put(i, generateRandomPerson());

        return map;
    }

    /** */
    public static Collection<CacheEntryImpl<Long, Person>> generateLongsPersonsEntries() {
        Collection<CacheEntryImpl<Long, Person>> entries = new LinkedList<>();

        for (long i = 0; i < BULK_OPERATION_SIZE; i++)
            entries.add(new CacheEntryImpl<>(i, generateRandomPerson()));

        return entries;
    }

    /** */
    public static Map<PersonId, Person> generatePersonIdsPersonsMap() {
        return generatePersonIdsPersonsMap(BULK_OPERATION_SIZE);
    }

    /** */
    public static Map<PersonId, Person> generatePersonIdsPersonsMap(int cnt) {
        Map<PersonId, Person> map = new HashMap<>();

        for (int i = 0; i < cnt; i++)
            map.put(generateRandomPersonId(), generateRandomPerson());

        return map;
    }

    /** */
    public static Collection<CacheEntryImpl<PersonId, Person>> generatePersonIdsPersonsEntries() {
        return generatePersonIdsPersonsEntries(BULK_OPERATION_SIZE);
    }

    /** */
    public static Collection<CacheEntryImpl<PersonId, Person>> generatePersonIdsPersonsEntries(int cnt) {
        Collection<CacheEntryImpl<PersonId, Person>> entries = new LinkedList<>();

        for (int i = 0; i < cnt; i++)
            entries.add(new CacheEntryImpl<>(generateRandomPersonId(), generateRandomPerson()));

        return entries;
    }

    /** */
    public static Person generateRandomPerson() {
        int phonesCnt = RANDOM.nextInt(4);

        List<String> phones = new LinkedList<>();

        for (int i = 0; i < phonesCnt; i++)
            phones.add(randomNumber(4));

        return new Person(randomString(4), randomString(4), RANDOM.nextInt(100),
            RANDOM.nextBoolean(), RANDOM.nextLong(), RANDOM.nextFloat(), new Date(), phones);
    }

    /** */
    public static PersonId generateRandomPersonId() {
        return new PersonId(randomString(4), randomString(4), RANDOM.nextInt(100));
    }

    /** */
    public static boolean checkMapsEqual(Map map1, Map map2) {
        if (map1 == null || map2 == null || map1.size() != map2.size())
            return false;

        for (Object key : map1.keySet()) {
            Object obj1 = map1.get(key);
            Object obj2 = map2.get(key);

            if (obj1 == null || obj2 == null || !obj1.equals(obj2))
                return false;
        }

        return true;
    }

    /** */
    public static <K, V> boolean checkCollectionsEqual(Map<K, V> map, Collection<CacheEntryImpl<K, V>> col) {
        if (map == null || col == null || map.size() != col.size())
            return false;

        for (CacheEntryImpl<K, V> entry : col) {
            if (!entry.getValue().equals(map.get(entry.getKey())))
                return false;
        }

        return true;
    }

    /** */
    public static <K> boolean checkPersonMapsEqual(Map<K, Person> map1, Map<K, Person> map2,
        boolean primitiveFieldsOnly) {
        if (map1 == null || map2 == null || map1.size() != map2.size())
            return false;

        for (K key : map1.keySet()) {
            Person person1 = map1.get(key);
            Person person2 = map2.get(key);

            boolean equals = person1 != null && person2 != null &&
                (primitiveFieldsOnly ? person1.equalsPrimitiveFields(person2) : person1.equals(person2));

            if (!equals)
                return false;
        }

        return true;
    }

    /** */
    public static <K> boolean checkPersonCollectionsEqual(Map<K, Person> map, Collection<CacheEntryImpl<K, Person>> col,
        boolean primitiveFieldsOnly) {
        if (map == null || col == null || map.size() != col.size())
            return false;

        for (CacheEntryImpl<K, Person> entry : col) {
            boolean equals = primitiveFieldsOnly ?
                entry.getValue().equalsPrimitiveFields(map.get(entry.getKey())) :
                entry.getValue().equals(map.get(entry.getKey()));

            if (!equals)
                return false;
        }

        return true;
    }


    public static Product generateRandomProduct() {
        float price = Float.parseFloat((1 + RANDOM.nextInt(99)) + "." + randomNumber(2));
        return new Product(Long.parseLong(randomNumber(10)), randomString(2), randomString(10), randomString(20), price);
    }

    public static Product generateRandomProduct(long id) {
        return new Product(id, randomString(3), randomString(10), randomString(20), generateProductPrice(id));
    }

    public static ProductOrder generateRandomOrder(Product product, long id) {
        Calendar cl = Calendar.getInstance();
        return new ProductOrder(product, id, cl.getTime(), 1 + RANDOM.nextInt(20));
    }

    public static ProductOrder generateRandomOrder(long productId, long id, Date date) {
        return new ProductOrder(productId, generateProductPrice(productId), id, date, 1 + RANDOM.nextInt(20));
    }

    /** */
    public static String randomString(int len) {
        StringBuilder builder = new StringBuilder(len);

        for (int i = 0; i < len; i++)
            builder.append(LETTERS_ALPHABET.charAt(RANDOM.nextInt(LETTERS_ALPHABET.length())));

        return builder.toString();
    }

    /** */
    public static String randomNumber(int len) {
        StringBuilder builder = new StringBuilder(len);

        for (int i = 0; i < len; i++)
            builder.append(NUMBERS_ALPHABET.charAt(RANDOM.nextInt(NUMBERS_ALPHABET.length())));

        return builder.toString();
    }

    private static float generateProductPrice(long id) {
        float price = Long.parseLong(Long.toString(id).replace("0", ""));

        int i = 0;

        while (price > 100) {
            if (i % 2 != 0)
                price = price / 2;
            else
                price = (float) Math.sqrt(price);

            i++;
        }

        return ((float)((int)(price * 100))) / 100.0F;
    }
}

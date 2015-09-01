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

package org.apache.ignite.internal.processors.cache.datastructures;

import java.util.UUID;
import org.apache.ignite.IgniteAtomicReference;
import org.apache.ignite.IgniteException;

/**
 * Basic tests for atomic reference.
 */
public abstract class GridCacheAtomicReferenceApiSelfAbstractTest extends IgniteAtomicsAbstractTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPrepareAtomicReference() throws Exception {
        /** Name of first atomic. */
        String atomicName1 = UUID.randomUUID().toString();

        /** Name of second atomic. */
        String atomicName2 = UUID.randomUUID().toString();

        String initVal = "1";
        IgniteAtomicReference<String> atomic1 = grid(0).atomicReference(atomicName1, initVal, true);
        IgniteAtomicReference<String> atomic2 = grid(0).atomicReference(atomicName2, null, true);

        assertNotNull(atomic1);
        assertNotNull(atomic2);

        atomic1.close();
        atomic2.close();

        atomic1.close();
        atomic2.close();

        assertNull(grid(0).atomicReference(atomicName1, null, false));
        assertNull(grid(0).atomicReference(atomicName2, null, false));

        try {
            atomic1.get();

            fail();
        }
        catch (IllegalStateException | IgniteException e) {
            info("Caught expected exception: " + e.getMessage());
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testSetAndGet() throws Exception {
        String atomicName = UUID.randomUUID().toString();

        String initVal = "qwerty";

        IgniteAtomicReference<String> atomic = grid(0).atomicReference(atomicName, initVal, true);

        assertEquals(initVal, atomic.get());

        atomic.set(null);

        assertEquals(null, atomic.get());
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testCompareAndSetSimpleValue() throws Exception {
        String atomicName = UUID.randomUUID().toString();

        String initVal = "qwerty";

        IgniteAtomicReference<String> atomic = grid(0).atomicReference(atomicName, initVal, true);

        assertEquals(initVal, atomic.get());

        atomic.compareAndSet("h", "j");

        assertEquals(initVal, atomic.get());

        atomic.compareAndSet(initVal, null);

        assertEquals(null, atomic.get());
    }
}
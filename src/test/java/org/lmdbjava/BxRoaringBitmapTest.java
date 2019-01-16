/*-
 * #%L
 * LmdbJava
 * %%
 * Copyright (C) 2016 - 2019 The LmdbJava Open Source Project
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

package org.lmdbjava;

import java.util.Random;

import org.roaringbitmap.RoaringBitmap;

import org.junit.Assert;
import org.junit.Test;

public class BxRoaringBitmapTest {
  @Test
  public void testGCStability() throws Throwable {
    final int N = 10000;
    final int M = 5000000;
    System.out.println("[testGCStability] testing GC stability with " + N + " bitmaps containing ~"
        + M / N + " values each on average");
    System.out.println("Universe size = " + M);
    final RoaringBitmap[] bitmaps = new RoaringBitmap[N];
    for (int i = 0; i < N; i++) {
      bitmaps[i] = new RoaringBitmap();
    }
    final Random random = new Random();
    for (int i = 0; i < M; i++) {
      final int x = random.nextInt(N);
      bitmaps[x].add(i);
    }
    int totalcard = 0;
    for (int i = 0; i < N; i++) {
      totalcard += bitmaps[i].getCardinality();
    }
    Assert.assertEquals(totalcard, M);
  }
}

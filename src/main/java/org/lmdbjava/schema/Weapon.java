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
// automatically generated by the FlatBuffers compiler, do not modify

package org.lmdbjava.schema;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")public final class Weapon extends Table {
  public static Weapon getRootAsWeapon(ByteBuffer _bb) { return getRootAsWeapon(_bb, new Weapon()); }
  public static Weapon getRootAsWeapon(ByteBuffer _bb, Weapon obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; }
  public Weapon __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public String name() { int o = __offset(4); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer nameAsByteBuffer() { return __vector_as_bytebuffer(4, 1); }
  public ByteBuffer nameInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 4, 1); }
  public short damage() { int o = __offset(6); return o != 0 ? bb.getShort(o + bb_pos) : 0; }

  public static int createWeapon(FlatBufferBuilder builder,
      int nameOffset,
      short damage) {
    builder.startObject(2);
    Weapon.addName(builder, nameOffset);
    Weapon.addDamage(builder, damage);
    return Weapon.endWeapon(builder);
  }

  public static void startWeapon(FlatBufferBuilder builder) { builder.startObject(2); }
  public static void addName(FlatBufferBuilder builder, int nameOffset) { builder.addOffset(0, nameOffset, 0); }
  public static void addDamage(FlatBufferBuilder builder, short damage) { builder.addShort(1, damage, 0); }
  public static int endWeapon(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
}

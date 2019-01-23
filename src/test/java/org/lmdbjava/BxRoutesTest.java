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

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.enums.CSVReaderNullFieldIndicator;

import com.google.flatbuffers.ByteBufferUtil;
import com.google.flatbuffers.FlatBufferBuilder;
import static com.google.flatbuffers.Constants.*;


import java.nio.ByteBuffer;
import static java.nio.ByteBuffer.allocateDirect;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.DbiFlags.MDB_DUPSORT;
import static org.lmdbjava.DbiFlags.MDB_INTEGERKEY;
import static org.lmdbjava.DirectBufferProxy.PROXY_DB;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.Env.open;
import static org.lmdbjava.GetOp.MDB_SET;
import static org.lmdbjava.SeekOp.MDB_FIRST;
import static org.lmdbjava.SeekOp.MDB_LAST;
import static org.lmdbjava.SeekOp.MDB_PREV;
import static org.lmdbjava.PutFlags.MDB_APPEND;
import static org.lmdbjava.PutFlags.MDB_APPENDDUP;


import org.lmdbjava.routes.*;

import org.junit.Assert;
import org.junit.Test;

public class BxRoutesTest {

    private CSVReader getCSVReader(String fileName) throws FileNotFoundException {
        InputStreamReader reader = new InputStreamReader(new FileInputStream(fileName), StandardCharsets.UTF_8);
        return new CSVReaderBuilder(reader).withFieldAsNull(CSVReaderNullFieldIndicator.EMPTY_SEPARATORS).build();
    } 



    // @Test
    public void testRandomWrite() throws IOException {

        final File path = new File("/tmp/bx/routes/");
        final Env<DirectBuffer> env = create(PROXY_DB)
            .setMapSize(10_485_760)
            .setMaxDbs(4)
            .open(path);

        final Dbi<DirectBuffer> db = env.openDbi("routesByOrigin", MDB_CREATE, MDB_INTEGERKEY);  //MDB_DUPSORT

        System.out.println("bx:maxKeySize:" + env.getMaxKeySize());
        int size = Integer.SIZE / Byte.SIZE;

        final ByteBuffer keyBb = allocateDirect(size); // allocateDirect(env.getMaxKeySize());
        // Agrona is faster than ByteBuffer and its methods are nicer...
        final MutableDirectBuffer key = new UnsafeBuffer(keyBb);
        final MutableDirectBuffer val = new UnsafeBuffer(allocateDirect(size)); // 1_000

        try (CSVReader csvReader = getCSVReader("/tmp/bx/routes/sorted_routes.csv");
             Txn<DirectBuffer> txn = env.txnWrite()) {

            try (Cursor<DirectBuffer> c = db.openCursor(txn)) {
                int i = 0;
                while (true) {
                    i++;
                    if (i > 5) {
                        break;
                    }
                    String[] lineTokens = csvReader.readNext();
                    if (lineTokens == null) {
                        break;
                    }
                    System.out.println(Arrays.toString(lineTokens));

                    String originNode = lineTokens[3];
                    int nodeId = Integer.parseInt(originNode, Character.MAX_RADIX);

                    int rand = (i % 2 == 1) ? 100 - i : i;
                    key.putInt(0, rand, ByteOrder.LITTLE_ENDIAN);
                    System.out.println("bx:key:" + originNode);
                    System.out.println("bx:nodeId:" + nodeId);
                    System.out.println("bx:keyCapacity:" + key.capacity());

                    val.putInt(0, i, ByteOrder.LITTLE_ENDIAN);
                    // val.putStringWithoutLengthUtf8(0, Arrays.toString(lineTokens));
                    c.put(key, val);  // MDB_APPENDDUP

                }
            }
            txn.commit();
        }

        env.close();

        System.out.println("Writing to MDB: completed successfully");
    }

    // @Test
    public void testSequentialRead() throws IOException {

        final File path = new File("/tmp/bx/routes/");
        final Env<DirectBuffer> env = create(PROXY_DB)
            .setMapSize(10_485_760)
            .setMaxDbs(4)
            .open(path);

        final Dbi<DirectBuffer> db = env.openDbi("routesByOrigin", MDB_INTEGERKEY);

        System.out.println("bx:maxKeySize:" + env.getMaxKeySize());

        final MutableDirectBuffer key = new UnsafeBuffer(allocateDirect(4));
        key.putInt(0, 95, ByteOrder.LITTLE_ENDIAN);

        // To fetch any data from LMDB we need a Txn. A Txn is very important in
        // LmdbJava because it offers ACID characteristics and internally holds a
        // read-only key buffer and read-only value buffer. These read-only buffers
        // are always the same two Java objects, but point to different LMDB-managed
        // memory as we use Dbi (and Cursor) methods. These read-only buffers remain
        // valid only until the Txn is released or the next Dbi or Cursor call. If
        // you need data afterwards, you should copy the bytes to your own buffer.
        try (Txn<DirectBuffer> txn = env.txnRead()) {
            final DirectBuffer found = db.get(txn, key);
                System.out.println("bx:found:" + found);
            if (found != null) {
                System.out.println("bx:found:" + found.getInt(0));
                System.out.println("bx:found:" + found.getInt(0, ByteOrder.LITTLE_ENDIAN));
            }

            // The fetchedVal is read-only and points to LMDB memory
            final DirectBuffer fetchedVal = txn.val();
                System.out.println("bx:fetchedVal:" + fetchedVal);
            if (fetchedVal != null) {
                System.out.println("bx:fetchedVal:" + fetchedVal.getInt(0));
                System.out.println("bx:fetchedVal:" + fetchedVal.getInt(0, ByteOrder.LITTLE_ENDIAN));
            }
        }

        env.close();

        System.out.println("Reading from MDB: completed successfully");
    }


    //----------------------------------------------------
    // #1: [LEX1, AMZL] 3 legs with PickupNotSupported
    // LEX1, AMTRAN | CVG9, AMZN_SORT | DTU1, AMZL

    // #2: [LEX1, USPS] 3 legs with PickupNotSupported
    // LEX1, AMTRAN | CVG9, AMZN_SORT | DTU1, USPS

    // #3: [LEX1, UPS] 1 legs with PickupNotSupported
    // LEX1, UPS

    // #4: [BWI5, AMZL] 3 legs with PickupNotSupported
    // BWI5, AMTRAN | CVG9, AMZN_SORT | DTU1, AMZL

    // #5: [BWI5, USPS] 3 legs with PickupNotSupported
    // BWI5, AMTRAN | CVG9, AMZN_SORT | DTU1, USPS

    // #6: [BWI5, AMZL] 1 legs with PickupNotSupported
    // BWI5, AMZL
    //----------------------------------------------------
    private void buildRoutes(FlatBufferBuilder fbb, boolean sizePrefix) {
        int[] nodes = { fbb.createString("LEX1"), fbb.createString("BWI5"), fbb.createString("CVG9"), fbb.createString("DTU1") };
        int[] methods = { fbb.createString("AMTRAN"), fbb.createString("AMZN_SORT"), fbb.createString("UPS"), fbb.createString("USPS"), fbb.createString("AMZL") };
        int[] legs = new int[3];
        int[] singleLegs = new int[1];
        int[] routes = new int[6];
        int legVector;

        // LEX1
        // adding individual legs for a given route
        LegInfo.startLegInfo(fbb);
        LegInfo.addSourceNode(fbb, nodes[0]);
        LegInfo.addShipMethodName(fbb, methods[0]);
        legs[0] = LegInfo.endLegInfo(fbb);
        LegInfo.startLegInfo(fbb);
        LegInfo.addSourceNode(fbb, nodes[2]);
        LegInfo.addShipMethodName(fbb, methods[1]);
        legs[1] = LegInfo.endLegInfo(fbb);
        LegInfo.startLegInfo(fbb);
        LegInfo.addSourceNode(fbb, nodes[3]);
        LegInfo.addShipMethodName(fbb, methods[4]);
        legs[2] = LegInfo.endLegInfo(fbb);
        legVector = RouteInfo.createLegNodesVector(fbb, legs);
        // done with adding legs for a given route
        RouteInfo.startRouteInfo(fbb);
        RouteInfo.addLegs(fbb, legVector);
        RouteInfo.addRouteAlias(fbb, methods[4]);
        RouteInfo.addRouteOrigin(fbb, nodes[0]);
        RouteInfo.addRouteId(fbb, 1);
        routes[0] = RouteInfo.endRouteInfo(fbb);

        // adding individual legs for a given route
        LegInfo.startLegInfo(fbb);
        LegInfo.addSourceNode(fbb, nodes[0]);
        LegInfo.addShipMethodName(fbb, methods[0]);
        legs[0] = LegInfo.endLegInfo(fbb);
        LegInfo.startLegInfo(fbb);
        LegInfo.addSourceNode(fbb, nodes[2]);
        LegInfo.addShipMethodName(fbb, methods[1]);
        legs[1] = LegInfo.endLegInfo(fbb);
        LegInfo.startLegInfo(fbb);
        LegInfo.addSourceNode(fbb, nodes[3]);
        LegInfo.addShipMethodName(fbb, methods[3]);
        legs[2] = LegInfo.endLegInfo(fbb);
        legVector = RouteInfo.createLegNodesVector(fbb, legs);
        // done with adding legs for a given route
        RouteInfo.startRouteInfo(fbb);
        RouteInfo.addLegs(fbb, legVector);
        RouteInfo.addRouteAlias(fbb, methods[3]);
        RouteInfo.addRouteOrigin(fbb, nodes[0]);
        RouteInfo.addRouteId(fbb, 2);
        routes[1] = RouteInfo.endRouteInfo(fbb);

        // adding individual legs for a given route
        LegInfo.startLegInfo(fbb);
        LegInfo.addSourceNode(fbb, nodes[0]);
        LegInfo.addShipMethodName(fbb, methods[2]);
        singleLegs[0] = LegInfo.endLegInfo(fbb);
        legVector = RouteInfo.createLegNodesVector(fbb, singleLegs);
        // done with adding legs for a given route
        RouteInfo.startRouteInfo(fbb);
        RouteInfo.addLegs(fbb, legVector);
        RouteInfo.addRouteAlias(fbb, methods[2]);
        RouteInfo.addRouteOrigin(fbb, nodes[0]);
        RouteInfo.addRouteId(fbb, 3);
        routes[2] = RouteInfo.endRouteInfo(fbb);


        // BWI5
        // adding individual legs for a given route
        LegInfo.startLegInfo(fbb);
        LegInfo.addSourceNode(fbb, nodes[1]);
        LegInfo.addShipMethodName(fbb, methods[0]);
        legs[0] = LegInfo.endLegInfo(fbb);
        LegInfo.startLegInfo(fbb);
        LegInfo.addSourceNode(fbb, nodes[2]);
        LegInfo.addShipMethodName(fbb, methods[1]);
        legs[1] = LegInfo.endLegInfo(fbb);
        LegInfo.startLegInfo(fbb);
        LegInfo.addSourceNode(fbb, nodes[3]);
        LegInfo.addShipMethodName(fbb, methods[4]);
        legs[2] = LegInfo.endLegInfo(fbb);
        legVector = RouteInfo.createLegNodesVector(fbb, legs);
        // done with adding legs for a given route
        RouteInfo.startRouteInfo(fbb);
        RouteInfo.addLegs(fbb, legVector);
        RouteInfo.addRouteAlias(fbb, methods[4]);
        RouteInfo.addRouteOrigin(fbb, nodes[1]);
        RouteInfo.addRouteId(fbb, 4);
        routes[3] = RouteInfo.endRouteInfo(fbb);

        // adding individual legs for a given route
        LegInfo.startLegInfo(fbb);
        LegInfo.addSourceNode(fbb, nodes[1]);
        LegInfo.addShipMethodName(fbb, methods[0]);
        legs[0] = LegInfo.endLegInfo(fbb);
        LegInfo.startLegInfo(fbb);
        LegInfo.addSourceNode(fbb, nodes[2]);
        LegInfo.addShipMethodName(fbb, methods[1]);
        legs[1] = LegInfo.endLegInfo(fbb);
        LegInfo.startLegInfo(fbb);
        LegInfo.addSourceNode(fbb, nodes[3]);
        LegInfo.addShipMethodName(fbb, methods[3]);
        legs[2] = LegInfo.endLegInfo(fbb);
        legVector = RouteInfo.createLegNodesVector(fbb, legs);
        // done with adding legs for a given route
        RouteInfo.startRouteInfo(fbb);
        RouteInfo.addLegs(fbb, legVector);
        RouteInfo.addRouteAlias(fbb, methods[3]);
        RouteInfo.addRouteOrigin(fbb, nodes[1]);
        RouteInfo.addRouteId(fbb, 5);
        routes[4] = RouteInfo.endRouteInfo(fbb);

        // adding individual legs for a given route
        LegInfo.startLegInfo(fbb);
        LegInfo.addSourceNode(fbb, nodes[1]);
        LegInfo.addShipMethodName(fbb, methods[4]);
        singleLegs[0] = LegInfo.endLegInfo(fbb);
        legVector = RouteInfo.createLegNodesVector(fbb, singleLegs);
        // done with adding legs for a given route
        RouteInfo.startRouteInfo(fbb);
        RouteInfo.addLegs(fbb, legVector);
        RouteInfo.addRouteAlias(fbb, methods[4]);
        RouteInfo.addRouteOrigin(fbb, nodes[1]);
        RouteInfo.addRouteId(fbb, 6);
        routes[5] = RouteInfo.endRouteInfo(fbb);

        int routeVector = Routes.createRoutesVector(fbb, routes);

        Routes.startRoutes(fbb);
        Routes.addRoutes(fbb, routeVector);
        int root = Routes.endRoutes(fbb);

        if (sizePrefix) {
            Routes.finishSizePrefixedRoutesBuffer(fbb, root);
        } else {
            Routes.finishRoutesBuffer(fbb, root);
        }
    }

    private void displayLeg(LegInfo legInfo) {
        System.out.print(legInfo.sourceNode() + ", " + legInfo.shipMethodName());
    }

    private void displayRoute(RouteInfo routeInfo) {
        System.out.println("\n\n#" + routeInfo.routeId() + ": [" + routeInfo.routeOrigin() + ", " + routeInfo.routeAlias() + "] "
                                 + routeInfo.legsLength() + " legs with " + PickupCapability.name(routeInfo.pickupCapability()));

        for (int i = 0; i < routeInfo.legsLength(); i++) {
            if (i > 0) {
                System.out.print(" | ");
            }
            displayLeg(routeInfo.legs(i));
        }
    }

    private void displayRoutes(ByteBuffer buffer) {
        Routes root = Routes.getRootAsRoutes(buffer);

        int numOfRoutes = root.routesLength();
        System.out.println("bx:routesLength:" + numOfRoutes);
        for (int i = 0; i < numOfRoutes; i++) {
            displayRoute(root.routes(i));
        }
    }

    // @Test
    public void testWriteValueAsFlatBuffer() throws IOException {

        // final class MappedByteBufferFactory implements FlatBufferBuilder.ByteBufferFactory {
        //     @Override
        //     public ByteBuffer newByteBuffer(int capacity) {
        //         ByteBuffer bb;
        //         try {
        //             bb =  new RandomAccessFile("/tmp/bx/routes/buffer.tmp", "rw").getChannel().map(FileChannel.MapMode.READ_WRITE, 0, capacity).order(ByteOrder.LITTLE_ENDIAN);
        //         } catch(Throwable e) {
        //             System.out.println("Failed to map ByteBuffer to a file");
        //             bb = null;
        //         }
        //         return bb;
        //     }
        // }
        // FlatBufferBuilder fbb = new FlatBufferBuilder(1, new MappedByteBufferFactory());
  
        FlatBufferBuilder fbb = new FlatBufferBuilder(allocateDirect(1_000_000));
        boolean sizePrefix = false;

        buildRoutes(fbb, sizePrefix);

        // Test it:
        ByteBuffer dataBuffer = fbb.dataBuffer();
        System.out.println("bx:dataBuffer.position:" + dataBuffer.position());
        System.out.println("bx:dataBuffer.limit:" + dataBuffer.limit());

        System.out.println("bx:sizePrefix:" + sizePrefix);
        System.out.println("bx:SIZE_PREFIX_LENGTH:" + SIZE_PREFIX_LENGTH);
        int totalSize = dataBuffer.remaining();
        System.out.println("bx:dataBuffer.totalSize:" + totalSize);
        System.out.println("bx:dataBuffer.isDirect:" + dataBuffer.isDirect());
        System.out.println("bx:dataBuffer.position:" + dataBuffer.position());
        System.out.println("bx:dataBuffer.limit:" + dataBuffer.limit());


        final File path = new File("/tmp/bx/routes/");
        final Env<ByteBuffer> env = create()
            .setMapSize(10_485_760)
            .setMaxDbs(4)
            .open(path);

        final Dbi<ByteBuffer> db = env.openDbi("routesByOrigin", MDB_CREATE);
        System.out.println("bx:maxKeySize:" + env.getMaxKeySize());

        final ByteBuffer key = allocateDirect(env.getMaxKeySize());
        key.put("routes.1".getBytes(StandardCharsets.UTF_8)).flip();
        db.put(key, dataBuffer);


        env.close();

        System.out.println("Writing flatbuffers to MDB: completed successfully");
    }

    @Test
    public void testReadValueAsFlatBuffer() throws IOException, InterruptedException {

        final File path = new File("/tmp/bx/routes/");
        final Env<ByteBuffer> env = create()
            .setMapSize(10_485_760)
            .setMaxDbs(4)
            .open(path);

        final Dbi<ByteBuffer> db = env.openDbi("routesByOrigin");

        System.out.println("bx:nativeOrder:" + ByteOrder.nativeOrder());
        System.out.println("bx:maxKeySize:" + env.getMaxKeySize());
        boolean sizePrefix = false;

        final ByteBuffer key = allocateDirect(env.getMaxKeySize());
        key.put("routes.1".getBytes(StandardCharsets.UTF_8)).flip();
        ByteBuffer dataBuffer = null;

        try (Txn<ByteBuffer> txn = env.txnRead()) {
            final ByteBuffer found = db.get(txn, key);
            System.out.println("bx:found:" + found);

            // The fetchedVal is read-only and points to LMDB memory
            final ByteBuffer fetchedVal = txn.val();
            System.out.println("bx:fetchedVal:" + fetchedVal);

            // Read data back from bytebuffer
            if (found != null) {
                dataBuffer = found.duplicate();
                int totalSize = dataBuffer.remaining();
                System.out.println("bx:dataBuffer.totalSize:" + totalSize);
                System.out.println("bx:dataBuffer.isDirect:" + dataBuffer.isDirect());
                System.out.println("bx:dataBuffer.position:" + dataBuffer.position());
                System.out.println("bx:dataBuffer.limit:" + dataBuffer.limit());
                if (sizePrefix) {
                    System.out.println("bx:dataBuffer.size:" + ByteBufferUtil.getSizePrefix(dataBuffer));
                    dataBuffer = ByteBufferUtil.removeSizePrefix(dataBuffer);
                }
                displayRoutes(dataBuffer);
            }
        }

 Thread.sleep(30000);
        env.close();
 Thread.sleep(30000);

                System.out.println("bx:dataBuffer.Long.BYTES:" + Long.BYTES);
                System.out.println("bx:dataBuffer.Long.BYTES*2:" + Long.BYTES * 2);

                System.out.println("bx:dataBuffer.isDirect:" + dataBuffer.isDirect());
                System.out.println("bx:dataBuffer.position:" + dataBuffer.position());
                System.out.println("bx:dataBuffer.limit:" + dataBuffer.limit());

        System.out.println("Reading flatbuffers from MDB: completed successfully");
    }


    // @Test
    // public void testReadValueAsFlatBuffer() throws IOException {

    //     final File path = new File("/tmp/bx/routes/");
    //     final Env<DirectBuffer> env = create(PROXY_DB)
    //         .setMapSize(10_485_760)
    //         .setMaxDbs(4)
    //         .open(path);

    //     final Dbi<DirectBuffer> db = env.openDbi("routesByOrigin");

    //     System.out.println("bx:maxKeySize:" + env.getMaxKeySize());
    //     boolean sizePrefix = true;

    //     final ByteBuffer keyBb = allocateDirect(env.getMaxKeySize());
    //     final MutableDirectBuffer key = new UnsafeBuffer(keyBb);
    //     int keyLen = key.putStringWithoutLengthUtf8(0, "routes.1");
    //     key.wrap(key, 0, keyLen);

    //     try (Txn<DirectBuffer> txn = env.txnRead()) {
    //         final DirectBuffer found = db.get(txn, key);
    //         System.out.println("bx:found:" + found);

    //         // The fetchedVal is read-only and points to LMDB memory
    //         final DirectBuffer fetchedVal = txn.val();
    //         System.out.println("bx:fetchedVal:" + fetchedVal);

    //         // Read data back from bytebuffer
    //         if (found != null) {
    //             ByteBuffer dataBuffer = found.byteBuffer();
    //             int totalSize = dataBuffer.remaining();
    //             System.out.println("bx:dataBuffer.totalSize:" + totalSize);
    //             System.out.println("bx:dataBuffer.isDirect:" + dataBuffer.isDirect());
    //             System.out.println("bx:dataBuffer.position:" + dataBuffer.position());
    //             System.out.println("bx:dataBuffer.limit:" + dataBuffer.limit());
    //             if (sizePrefix) {
    //                 System.out.println("bx:dataBuffer.size:" + ByteBufferUtil.getSizePrefix(dataBuffer));
    //                 dataBuffer = ByteBufferUtil.removeSizePrefix(dataBuffer);
    //             }
    //             displayRoutes(dataBuffer);
    //         }
    //     }

    //     env.close();

    //     System.out.println("Reading flatbuffers from MDB: completed successfully");
    // }


}

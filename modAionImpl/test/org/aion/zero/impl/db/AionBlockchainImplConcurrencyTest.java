/*******************************************************************************
 * Copyright (c) 2017-2018 Aion foundation.
 *
 *     This file is part of the aion network project.
 *
 *     The aion network project is free software: you can redistribute it
 *     and/or modify it under the terms of the GNU General Public License
 *     as published by the Free Software Foundation, either version 3 of
 *     the License, or any later version.
 *
 *     The aion network project is distributed in the hope that it will
 *     be useful, but WITHOUT ANY WARRANTY; without even the implied
 *     warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *     See the GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with the aion network project source files.
 *     If not, see <https://www.gnu.org/licenses/>.
 *
 *     The aion network project leverages useful source code from other
 *     open source projects. We greatly appreciate the effort that was
 *     invested in these projects and we thank the individual contributors
 *     for their work. For provenance information and contributors
 *     please see <https://github.com/aionnetwork/aion/wiki/Contributors>.
 *
 * Contributors to the aion source files in decreasing order of code volume:
 *     Aion foundation.
 ******************************************************************************/
package org.aion.zero.impl.db;

import org.aion.log.AionLoggerFactory;
import org.aion.mcf.core.ImportResult;
import org.aion.zero.impl.StandaloneBlockchain;
import org.aion.zero.impl.types.AionBlock;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;
import static org.aion.db.impl.DatabaseTestUtils.assertConcurrent;

/**
 * @author Alexandra Roatis
 */
public class AionBlockchainImplConcurrencyTest {

    private static final int CONCURRENT_THREADS_PER_TYPE = 300;
    private static final int TIME_OUT = 100; // in seconds
    private static final boolean DISPLAY_MESSAGES = true;

    private static StandaloneBlockchain chain;
    private static StandaloneBlockchain sourceChain;
    private static List<AionBlock> parents = new ArrayList<>();

    @BeforeClass
    public static void setup() {
        Map<String, String> cfg = new HashMap<>();
        cfg.put("DB", "ERROR");

        AionLoggerFactory.init(cfg);

        // build a blockchain with a few blocks
        StandaloneBlockchain.Builder builder = new StandaloneBlockchain.Builder();
        StandaloneBlockchain.Bundle bundle = builder.withValidatorConfiguration("simple").build();

        chain = bundle.bc;

        // build a blockchain with a few blocks
        builder = new StandaloneBlockchain.Builder();
        sourceChain = builder.withValidatorConfiguration("simple").build().bc;

        generateBlocks();
    }

    private static void generateBlocks() {
        Random rand = new Random();
        AionBlock parent, block, mainChain;
        mainChain = sourceChain.getGenesis();
        parents.add(mainChain);

        for (int i = 0; i < CONCURRENT_THREADS_PER_TYPE; i++) {

            // adding to the main chain every 5th block or at random
            if (i % 5 == 0) {
                parent = mainChain;
            } else {
                parent = parents.get(rand.nextInt(parents.size()));
            }

            block = sourceChain.createNewBlock(parent, Collections.emptyList(), true);
            block.setExtraData(String.valueOf(i).getBytes());

            ImportResult result = sourceChain.tryToConnect(block);
            parents.add(block);
            if (result == ImportResult.IMPORTED_BEST) {
                mainChain = block;
            }

            if (DISPLAY_MESSAGES) {
                System.out.println("Created Block: (hash: " + block.getShortHash() + ", number: " + block.getNumber()
                                           + ", extra data: " + new String(block.getExtraData()) + ") == " + result
                        .toString());
            }
        }

        parents.remove(sourceChain.getGenesis());
    }

    @AfterClass
    public static void teardown() {
        chain.close();
    }

    private void addThread_tryToConnect(List<Runnable> _threads,
                                        AionBlock _block,
                                        AionBlockStore store,
                                        AtomicBoolean done) {
        _threads.add(() -> {
            ImportResult result = chain.tryToConnect(_block);
            if (DISPLAY_MESSAGES) {
                System.out.println(Thread.currentThread().getName() + ": tryToConnect(hash: " + _block.getShortHash()
                                           + ", number: " + _block.getNumber() + ", extra data: "
                                           + new String(_block.getExtraData()) + ") == " + result.toString());
            }
            if (result == ImportResult.IMPORTED_BEST || result == ImportResult.IMPORTED_NOT_BEST) {
                assertThat(store.getTotalDifficultyForHash(_block.getHash()))
                        .isEqualTo(store.getTotalDifficultyForHash(_block.getParentHash())
                                           .add(_block.getDifficultyBI()));
                if (result == ImportResult.IMPORTED_BEST) {
                    assertThat(store.getTotalDifficulty()).isAtLeast(store.getTotalDifficultyForHash(_block.getHash()));
                }
            }
            if (result == ImportResult.NO_PARENT) {
                done.set(false);
            }
        });
    }

    private void addThread_createNewBlock(List<Runnable> _threads,
                                          AionBlock _parent,
                                          int blockCount,
                                          ConcurrentLinkedQueue<AionBlock> queue,
                                          AtomicBoolean done) {
        _threads.add(() -> {
            if (chain.isBlockExist(_parent.getHash())) {
                AionBlock block = chain.createNewBlock(_parent, Collections.emptyList(), true);
                block.setExtraData(String.valueOf(blockCount).getBytes());

                if (!chain.isBlockExist(block.getHash())) {
                    // still adding this block
                    queue.add(block);

                    if (DISPLAY_MESSAGES) {
                        System.out.println(
                                Thread.currentThread().getName() + ": createNewBlock(hash: " + block.getShortHash()
                                        + ", number: " + block.getNumber() + ", extra data: "
                                        + new String(block.getExtraData()) + ", parent: " + _parent.getShortHash()
                                        + ")");
                    }
                }
            } else {
                done.set(false);
            }
        });
    }

    private void addThread_createNewBlock_mainChain(List<Runnable> _threads,
                                                    int blockCount,
                                                    ConcurrentLinkedQueue<AionBlock> queue,
                                                    AtomicInteger threadCount,
                                                    AtomicBoolean done) {
        _threads.add(() -> {
            AionBlock _parent = chain.getBestBlock();

            if (_parent.getNumber() <= CONCURRENT_THREADS_PER_TYPE) {
                done.set(false);
            }
            if (threadCount.get() < CONCURRENT_THREADS_PER_TYPE && _parent.getNumber() > CONCURRENT_THREADS_PER_TYPE) {
                AionBlock block = chain.createNewBlock(_parent, Collections.emptyList(), true);
                block.setExtraData(String.valueOf(blockCount).getBytes());

                if (!chain.isBlockExist(block.getHash())) {
                    // still adding this block
                    queue.add(block);

                    if (DISPLAY_MESSAGES) {
                        System.out.println(
                                Thread.currentThread().getName() + ": createNewBlock_mainChain(hash: " + block
                                        .getShortHash() + ", number: " + block.getNumber() + ", extra data: "
                                        + new String(block.getExtraData()) + ", parent: " + _parent.getShortHash()
                                        + ")");
                    }
                }

                threadCount.getAndIncrement();
                done.set(false);
            }

        });
    }

    private void addThread_tryToConnect_fromQueue(List<Runnable> _threads,
                                                  ConcurrentLinkedQueue<AionBlock> queue,
                                                  ConcurrentLinkedQueue<AionBlock> imported,
                                                  AionBlockStore store,
                                                  AtomicBoolean done) {
        _threads.add(() -> {
            AionBlock _block = queue.poll();
            if (_block != null) {
                ImportResult result = chain.tryToConnect(_block);
                if (DISPLAY_MESSAGES) {
                    System.out.println(
                            Thread.currentThread().getName() + ": tryToConnect_fromQueue(hash: " + _block.getShortHash()
                                    + ", number: " + _block.getNumber() + ", extra data: "
                                    + new String(_block.getExtraData()) + ") == " + result.toString());
                }
                if (result == ImportResult.IMPORTED_BEST || result == ImportResult.IMPORTED_NOT_BEST) {
                    assertThat(store.getTotalDifficultyForHash(_block.getHash()))
                            .isEqualTo(store.getTotalDifficultyForHash(_block.getParentHash())
                                               .add(_block.getDifficultyBI()));
                    if (result == ImportResult.IMPORTED_BEST) {
                        assertThat(store.getTotalDifficulty())
                                .isAtLeast(store.getTotalDifficultyForHash(_block.getHash()));
                    }
                    imported.add(_block);
                }
                if (result == ImportResult.NO_PARENT) {
                    done.set(false);
                }
            }
        });
    }

    @Test
    public void testConcurrent_tryToConnect() throws InterruptedException {
        List<Runnable> threads = new ArrayList<>();
        AionBlockStore store = chain.getBlockStore();
        AtomicBoolean done = new AtomicBoolean(false);
        AtomicInteger addCount = new AtomicInteger(0);
        ConcurrentLinkedQueue<AionBlock> queue = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<AionBlock> imported = new ConcurrentLinkedQueue<>();

        int blockCount = CONCURRENT_THREADS_PER_TYPE + 1;
        for (AionBlock blk : parents) {
            // connect to known blocks
            addThread_tryToConnect(threads, blk, store, done);

            // add new blocks
            addThread_createNewBlock(threads, blk, blockCount, queue, done);
            blockCount++;
            // add new blocks
            addThread_createNewBlock_mainChain(threads, blockCount, queue, addCount, done);
            blockCount++;

            // connect to new blocks
            addThread_tryToConnect_fromQueue(threads, queue, imported, store, done);
        }

        // run threads
        while (!done.get()) {
            done.set(true);
            assertConcurrent("Testing tryToConnect(...) ", threads, TIME_OUT);
        }

        // adding new blocks to source chain
        AionBlock block = imported.poll();
        while (block != null) {
            ImportResult result = sourceChain.tryToConnect(block);
            parents.add(block);
            if (DISPLAY_MESSAGES) {
                System.out.println("Importing block (hash: " + block.getShortHash() + ", number: " + block.getNumber()
                                           + ", extra data: " + new String(block.getExtraData()) + ") == " + result
                        .toString());
            }
            block = imported.poll();
        }

        // comparing total diff for the two chains
        assertThat(chain.getTotalDifficulty()).isEqualTo(sourceChain.getTotalDifficulty());

        AionBlockStore sourceStore = sourceChain.getBlockStore();

        // comparing total diff for each block of the two chains
        for (AionBlock blk : parents) {
            assertThat(store.getTotalDifficultyForHash(blk.getHash()))
                    .isEqualTo(sourceStore.getTotalDifficultyForHash(blk.getHash()));
        }
    }
}
